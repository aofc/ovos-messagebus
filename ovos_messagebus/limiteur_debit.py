# 202home : anti-déluge par connexion — voir MessageBusEventHandler.on_message()
# dans event_handler.py, qui est le seul appelant. Logique pure, sans Tornado
# ni websocket, pour rester testable directement.
"""Limiteur de débit par connexion du bus.

Le bus d'origine rediffuse chaque message reçu à tous les clients connectés,
sans aucune limite (voir `client_connections` dans event_handler.py) — un
seul processus qui se met à boucler (reconnexion en rafale, bug applicatif)
inonde instantanément tout le monde : GUI, skills, chaque pont MQTT.
Constaté en vrai, dans ce projet : 3000+ messages en 2 secondes à la
connexion d'un simple abonné, un même message d'état répété des dizaines de
fois d'affilée.

Règle : au plus UN message d'un type donné toutes les DELAI_MEME_TYPE
secondes, jamais plus de DEBIT_MAX émissions par seconde au total pour cette
connexion. Au-delà, le message est mis en FILE — jamais perdu, juste étalé
dans le temps — jusqu'à FILE_MAX. Passé ce seuil, ce n'est plus une rafale
normale (le chargement d'un skill, par exemple, envoie des centaines de
`register_vocab` d'affilée, mais tous DIFFÉRENTS — jamais bloqué par la
DEUXIÈME borne ci-dessous, seulement lissé par la première) : c'est un
client fautif, à l'appelant de fermer la connexion (voir sature()).
"""
from collections import deque


class LimiteurDebit:

    def __init__(self, delai_meme_type=0.2, debit_max=10, file_max=200):
        self.delai_meme_type = delai_meme_type
        self.debit_max = debit_max
        self.file_max = file_max
        # type -> horodatage (time.monotonic()) de sa dernière émission.
        self._dernier_envoi_type = {}
        # Horodatages des émissions de la dernière seconde, PAS un simple
        # compteur : une fenêtre glissante, pour que le débit se mesure sur
        # N'IMPORTE QUELLE seconde écoulée, pas seulement depuis un instant
        # de remise à zéro arbitraire.
        self._horodatages_recents = deque()
        # [(type, message)] dans l'ordre d'arrivée. Un type bloqué ne doit
        # JAMAIS retarder un autre type derrière lui dans la file (sans quoi
        # une rafale sur "register_vocab" affamerait "availability") —
        # purger_eligibles() teste chaque entrée individuellement, pas
        # seulement la tête de file.
        self._file = deque()

    def _sous_debit_global(self, maintenant):
        while self._horodatages_recents and maintenant - self._horodatages_recents[0] >= 1.0:
            self._horodatages_recents.popleft()
        return len(self._horodatages_recents) < self.debit_max

    def _type_disponible(self, type_, maintenant):
        dernier = self._dernier_envoi_type.get(type_)
        return dernier is None or maintenant - dernier >= self.delai_meme_type

    def _enregistrer_envoi(self, type_, maintenant):
        self._horodatages_recents.append(maintenant)
        self._dernier_envoi_type[type_] = maintenant

    def autoriser(self, type_, maintenant) -> bool:
        """True si CE message peut partir immédiatement — et dans ce cas
        SEULEMENT, l'émission est enregistrée : l'appelant n'a rien d'autre
        à tenir à jour. Faux si un message de même type est déjà en file :
        l'ORDRE d'arrivée par type doit être préservé, un nouveau message ne
        double jamais ceux qui attendent déjà."""
        if any(t == type_ for t, _ in self._file):
            return False
        if self._sous_debit_global(maintenant) and self._type_disponible(type_, maintenant):
            self._enregistrer_envoi(type_, maintenant)
            return True
        return False

    def mettre_en_file(self, type_, message):
        self._file.append((type_, message))

    def sature(self) -> bool:
        return len(self._file) > self.file_max

    def file_pleine(self) -> bool:
        return bool(self._file)

    def taille_file(self) -> int:
        return len(self._file)

    def purger_eligibles(self, maintenant) -> list:
        """Les messages désormais autorisés à sortir, dans leur ordre
        d'arrivée respectif — mais un type encore bloqué n'empêche pas un
        AUTRE type, plus loin dans la file, de sortir avant lui."""
        sortis = []
        reste = deque()
        while self._file:
            type_, message = self._file.popleft()
            if self._sous_debit_global(maintenant) and self._type_disponible(type_, maintenant):
                self._enregistrer_envoi(type_, maintenant)
                sortis.append(message)
            else:
                reste.append((type_, message))
        self._file = reste
        return sortis
