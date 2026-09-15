"""LimiteurDebit — anti-déluge par connexion du bus.

    python3 tests/test_limiteur_debit.py
"""
import os
import sys

RACINE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, RACINE)

from ovos_messagebus.limiteur_debit import LimiteurDebit  # noqa: E402


def executer():
    echecs, passes = [], []

    def verifier(nom, obtenu, attendu):
        ok = obtenu == attendu
        (passes if ok else echecs).append(
            nom if ok else f"{nom}: attendu {attendu!r}, obtenu {obtenu!r}")
        print(f"  {'OK  ' if ok else 'ÉCHEC'} {nom}")

    # --- débit normal : un seul message, jamais throttlé ----------------------
    l = LimiteurDebit(delai_meme_type=0.2, debit_max=10, file_max=5)
    verifier("un message isolé part immédiatement", l.autoriser("t", 0.0), True)
    verifier("aucune file après un envoi immédiat", l.file_pleine(), False)

    # --- 200ms entre deux messages du MÊME type --------------------------------
    l = LimiteurDebit(delai_meme_type=0.2, debit_max=10, file_max=50)
    verifier("premier message du type A part", l.autoriser("A", 0.0), True)
    verifier("deuxième A à 0.05s est refusé (< 200ms)", l.autoriser("A", 0.05), False)
    l.mettre_en_file("A", "msgA2")
    verifier("A à 0.05s a rejoint la file", l.taille_file(), 1)
    verifier("A à 0.05s (déjà en file) est refusé même s'il repasse",
             l.autoriser("A", 0.05), False)
    sortis = l.purger_eligibles(0.19)
    verifier("rien ne sort avant 200ms révolues", sortis, [])
    sortis = l.purger_eligibles(0.21)
    verifier("A sort une fois les 200ms écoulées", sortis, ["msgA2"])
    verifier("la file est vide après la purge", l.file_pleine(), False)

    # --- rafale de MÊME type, payloads DIFFÉRENTS (register_vocab) : rien perdu
    l = LimiteurDebit(delai_meme_type=0.2, debit_max=10, file_max=500)
    t = 0.0
    premier = l.autoriser("register_vocab", t)
    verifier("le tout premier register_vocab part immédiatement", premier, True)
    for i in range(1, 50):
        if not l.autoriser("register_vocab", t):
            l.mettre_en_file("register_vocab", f"vocab-{i}")
    verifier("49 messages en attente après la rafale", l.taille_file(), 49)
    recus = []
    tour, t = 0, 0.0
    while l.file_pleine() and tour < 500:
        tour += 1
        t = tour * 0.2
        recus.extend(l.purger_eligibles(t))
    verifier("les 49 messages en file finissent TOUS par sortir",
             sorted(recus), sorted(f"vocab-{i}" for i in range(1, 50)))
    verifier("l'ordre PAR TYPE est respecté (FIFO)", recus,
             [f"vocab-{i}" for i in range(1, 50)])

    # --- débit global : jamais plus de N émissions/seconde, tous types confondus
    l = LimiteurDebit(delai_meme_type=0.0, debit_max=3, file_max=50)
    resultats = [l.autoriser(f"type-{i}", 0.0) for i in range(5)]
    verifier("seuls les 3 premiers (débit global) partent immédiatement",
             resultats, [True, True, True, False, False])

    # --- un type bloqué ne doit PAS affamer un autre type derrière lui --------
    l = LimiteurDebit(delai_meme_type=0.2, debit_max=10, file_max=50)
    l.autoriser("availability", 0.0)                      # occupe availability
    l.mettre_en_file("availability", "dispo-2")            # bloqué 200ms
    l.mettre_en_file("cluster_topics", "topics-1")          # type DIFFÉRENT, libre
    sortis = l.purger_eligibles(0.01)
    verifier("un type différent sort même si un autre reste bloqué",
             sortis, ["topics-1"])
    verifier("le type bloqué reste en file, lui", l.taille_file(), 1)

    # --- sature() : signal de client fautif, pas une rafale normale -----------
    l = LimiteurDebit(delai_meme_type=1.0, debit_max=1, file_max=3)
    l.autoriser("x", 0.0)
    for i in range(3):
        l.mettre_en_file("x", f"x{i}")
    verifier("file au plafond : pas encore saturée", l.sature(), False)
    l.mettre_en_file("x", "x-de-trop")
    verifier("un message de plus que FILE_MAX : saturée", l.sature(), True)

    print(f"\n  {len(passes)} contrôles passés" +
          (f", {len(echecs)} échec(s) : " + "; ".join(echecs) if echecs else "."))
    return not echecs


if __name__ == "__main__":
    sys.exit(0 if executer() else 1)
