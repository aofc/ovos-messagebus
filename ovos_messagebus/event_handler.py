# Copyright 2017 Mycroft AI Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Define the web socket event handler for the message bus."""
import json
import sys
import time
import traceback

from ovos_bus_client.message import Message
from ovos_bus_client.session import SessionManager
from ovos_config import Configuration
from ovos_utils.log import LOG
from pyee import EventEmitter
from tornado.ioloop import IOLoop
from tornado.websocket import WebSocketHandler

from ovos_messagebus.limiteur_debit import LimiteurDebit

client_connections = []


class MessageBusEventHandler(WebSocketHandler):
    def __init__(self, application, request, **kwargs):
        super().__init__(application, request, **kwargs)
        self.emitter = EventEmitter()
        # 202home : un limiteur PAR CONNEXION — self EST l'émetteur, voir
        # limiteur_debit.py pour le pourquoi. self._purge_programmee évite
        # d'empiler plusieurs rappels différés pour la même connexion :
        # _purger() se reprogramme lui-même tant que la file n'est pas vide.
        self._limiteur = LimiteurDebit(self.rate_limit_delai_type,
                                       self.rate_limit_debit_max,
                                       self.rate_limit_file_max)
        self._purge_programmee = False

    def on(self, event_name, handler):
        self.emitter.on(event_name, handler)

    @property
    def filter(self) -> bool:
        return Configuration().get("websocket", {}).get("filter", False)

    @property
    def filter_logs(self) -> list:
        return Configuration().get("websocket", {}).get("filter_logs", ["gui.status.request", "gui.page.upload"])

    @property
    def max_message_size(self) -> int:
        return Configuration().get("websocket", {}).get("max_msg_size", 10) * 1024 * 1024

    # 202home : réglages du limiteur de débit — mêmes valeurs par défaut que
    # ce que l'utilisateur a fixé en session (200 ms entre deux messages
    # d'un même type, 10/s au total par émetteur, file de 200 avant de
    # considérer que ce n'est plus une rafale normale). Ajustables sans
    # repatcher, comme filter/max_msg_size ci-dessus.
    @property
    def rate_limit_delai_type(self) -> float:
        return Configuration().get("websocket", {}).get("rate_limit_delai_type", 0.2)

    @property
    def rate_limit_debit_max(self) -> int:
        return Configuration().get("websocket", {}).get("rate_limit_debit_max", 10)

    @property
    def rate_limit_file_max(self) -> int:
        return Configuration().get("websocket", {}).get("rate_limit_file_max", 200)

    def on_message(self, message):
        # 202home : anti-déluge AVANT toute diffusion — le type doit être lu
        # même hors du mode `filter` (qui, seul, désérialisait déjà avant
        # cette version). Un message illisible (type_ absent) n'est jamais
        # throttlé : on ne peut pas le grouper par type, et le comportement
        # d'origine (transmis tel quel) reste le repli le plus sûr.
        type_ = self._type_du_message(message)
        if type_ is not None:
            maintenant = time.monotonic()
            if not self._limiteur.autoriser(type_, maintenant):
                self._limiteur.mettre_en_file(type_, message)
                if self._limiteur.sature():
                    LOG.warning(
                        "messagebus : débit excessif sur '%s' depuis %s — "
                        "connexion fermée (%d messages en file)",
                        type_, self.request.remote_ip, self._limiteur.taille_file())
                    self.close(code=1008, reason=f"débit excessif : {type_}")
                    return
                self._programmer_purge()
                return
        self._diffuser(message)

    @staticmethod
    def _type_du_message(message):
        if not isinstance(message, str):
            return None
        try:
            deserialise = Message.deserialize(message)
        except Exception:
            return None
        return deserialise.msg_type

    def _programmer_purge(self):
        if self._purge_programmee:
            return
        self._purge_programmee = True
        IOLoop.current().call_later(self.rate_limit_delai_type, self._purger)

    def _purger(self):
        self._purge_programmee = False
        maintenant = time.monotonic()
        for message in self._limiteur.purger_eligibles(maintenant):
            self._diffuser(message)
        # La file peut avoir grandi pendant la purge (nouveaux messages
        # arrivés entre-temps) : on_message() l'a déjà reprogrammée le cas
        # échéant, mais purger_eligibles() peut laisser des entrées non
        # écoulées (débit global atteint) sans qu'on_message() en soit
        # informé — c'est ICI qu'il faut le vérifier, pas seulement à
        # l'arrivée d'un nouveau message.
        if self._limiteur.file_pleine():
            self._programmer_purge()

    def _diffuser(self, message):
        """Le comportement d'origine, inchangé : dispatch local puis
        rediffusion à TOUS les clients connectés — voir client_connections.
        Extrait de on_message() pour être appelé aussi bien immédiatement
        (chemin rapide) qu'en différé, depuis _purger()."""
        if not self.filter:
            try:
                self.emitter.emit(message)
            except Exception as e:
                LOG.exception(e)
                traceback.print_exc(file=sys.stdout)
                pass
        else:
            try:
                deserialized_message = Message.deserialize(message)
            except Exception:
                return

            if deserialized_message.msg_type not in self.filter_logs:
                LOG.debug(deserialized_message.msg_type +
                          f' source: {deserialized_message.context.get("source", [])}' +
                          f' destination: {deserialized_message.context.get("destination", [])}\n'
                          f'SESSION: {SessionManager.get(deserialized_message).serialize()}')

            try:
                self.emitter.emit(deserialized_message.msg_type, deserialized_message)
            except Exception as e:
                LOG.exception(e)
                traceback.print_exc(file=sys.stdout)
                pass

        for client in client_connections:
            client.write_message(message)

    def open(self):
        self.write_message(Message("connected",
                                   context={"session": {"session_id": "default"}}).serialize())
        client_connections.append(self)

    def on_close(self):
        client_connections.remove(self)

    def emit(self, channel_message):
        if (hasattr(channel_message, 'serialize') and
                callable(getattr(channel_message, 'serialize'))):
            self.write_message(channel_message.serialize())
        else:
            self.write_message(json.dumps(channel_message))

    def check_origin(self, origin):
        return True
