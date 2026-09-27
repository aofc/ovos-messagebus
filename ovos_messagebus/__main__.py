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
""" Message bus service for mycroft-core

The message bus facilitates inter-process communication between mycroft-core
processes. It implements a websocket server so can also be used by external
systems to integrate with the Mycroft system.
"""

import os
import ssl

from ovos_utils import create_daemon, wait_for_exit_signal
from ovos_messagebus.load_config import load_message_bus_config
from ovos_utils.log import LOG, init_service_logger
from ovos_utils.process_utils import reset_sigint_handler
from tornado import web, ioloop

from ovos_messagebus.event_handler import MessageBusEventHandler

# 202home : même identité TLS que les autres serveurs de coeur (ovos_microphone_plugin_navigateur,
# hp_navigateur) — voir outils/generer-certs-mqtt.sh et le montage docker-compose.yml/.jeedom.yml. `ssl`
# (config.ssl, ci-dessous) est un réglage OVOS DÉJÀ existant, lu par ovos_bus_client (tous les skills) —
# jamais branché côté serveur jusqu'ici, d'où le http:// (ws://) resté nu malgré ce réglage.
_TLS_CERT_FICHIER = os.environ.get("MYCROFT_COEUR_TLS_CERT", "/etc/202home/coeur-tls/coeur.crt")
_TLS_KEY_FICHIER = os.environ.get("MYCROFT_COEUR_TLS_KEY", "/etc/202home/coeur-tls/coeur.key")


# SÉCURITÉ — À GARDER (doc/securite.md) :
# le bus n'écoute QU'EN TLS (certificat coeur signé par la CA du cluster). Pas de repli en ws://.
def _contexte_tls():
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(_TLS_CERT_FICHIER, _TLS_KEY_FICHIER)
    return ctx


def on_ready():
    LOG.info('Message bus service started!')


def on_error(e='Unknown'):
    LOG.info('Message bus failed to start ({})'.format(repr(e)))


def on_stopping():
    LOG.info('Message bus is shutting down...')


def main(ready_hook=on_ready, error_hook=on_error, stopping_hook=on_stopping):
    reset_sigint_handler()
    init_service_logger("bus")
    LOG.info('Starting message bus service...')
    config = load_message_bus_config()
    routes = [(config.route, MessageBusEventHandler)]
    application = web.Application(routes)
    # ÉCHEC FERMÉ, comme les autres serveurs de coeur : si config.ssl est vrai mais le certificat
    # manque/est illisible, laisser lever plutôt que retomber sur du ws:// nu sans que rien ne le
    # signale. config.ssl à faux (par défaut si absent du réglage OVOS) garde le comportement d'origine.
    ssl_options = _contexte_tls() if config.ssl else None
    application.listen(config.port, config.host, ssl_options=ssl_options)
    create_daemon(ioloop.IOLoop.instance().start)
    ready_hook()
    wait_for_exit_signal()
    stopping_hook()


if __name__ == "__main__":
    main()
