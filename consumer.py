import pandas as pd
from kafka import KafkaConsumer
import json
import os

# Fonction de consommation en streaming de Kafka et de sauvegarde dans un DataFrame
def consume_and_save_to_csv(topic, output_file, bootstrap_servers='localhost:9092', batch_size=1000):
    # Assurez-vous que le dossier pour le fichier de sortie existe
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    # Créer un consommateur Kafka
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',  # Commencer à lire depuis le début si le consommateur est nouveau
        enable_auto_commit=True,  # Commit automatique des offsets
        group_id=None,  # Sans groupe de consommateurs
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),  # Désérialisation des messages en JSON
        fetch_max_wait_ms=500,  # Attente maximale pour collecter des messages
        max_poll_records=batch_size  # Lire jusqu'à `batch_size` messages à la fois
    )

    print(f"Consommateur connecté à Kafka sur {bootstrap_servers} pour le topic {topic}...")

    # Liste pour accumuler les messages reçus
    accumulated_data = []
    compteur = 1  # Compteur pour les fichiers partiels
    idle_time = 0  # Compteur d'inactivité

    try:
        while True:
            # Lire les messages de Kafka
            messages = consumer.poll(timeout_ms=1000)  # Timeout de 1 seconde

            # Vérifier s'il y a des messages
            if messages:
                idle_time = 0  # Réinitialiser le compteur d'inactivité

                for _, records in messages.items():
                    for message in records:
                        try:
                            # Désérialiser et ajouter le message aux données accumulées
                            data = message.value
                            print(f"Message reçu : {data}")
                            accumulated_data.append(data)
                        except Exception as e:
                            print(f"Erreur lors de la désérialisation du message : {e}")

                # Sauvegarder les données lorsque le batch est plein
                if len(accumulated_data) >= batch_size:
                    save_to_csv(accumulated_data, output_file, compteur)
                    compteur += 1
                    accumulated_data = []  # Réinitialiser les données accumulées

            else:
                # Incrémenter le compteur d'inactivité
                idle_time += 1
                print(f"Inactivité pendant {idle_time} secondes...")

                # Sauvegarder les données restantes si inactif pendant 15 secondes
                if idle_time >= 15:
                    if accumulated_data:
                        save_to_csv(accumulated_data, output_file, compteur)
                    consumer.close()  # Fermer le consommateur
                    print("Consommateur Kafka fermé.")
                    break

    except KeyboardInterrupt:
        print("Arrêt manuel du consommateur.")
        if accumulated_data:
            save_to_csv(accumulated_data, output_file, compteur)
        consumer.close()

# Fonction utilitaire pour sauvegarder des données dans un fichier CSV
def save_to_csv(data, output_file, compteur):
    try:
        new_data = pd.DataFrame(data)
        partial_file = f'{output_file}_{compteur}.csv'

        # Sauvegarder les données dans un fichier partiel
        new_data.to_csv(partial_file, index=False)
        print(f"Fichier partiel sauvegardé : {partial_file}")

        # Sauvegarder en mode append dans le fichier principal
        if os.path.exists(f'{output_file}.csv'):
            new_data.to_csv(f'{output_file}.csv', mode='a', header=False, index=False)
        else:
            new_data.to_csv(f'{output_file}.csv', index=False)
        print(f"Les données ont été ajoutées au fichier principal : {output_file}.csv")
    except Exception as e:
        print(f"Erreur lors de la sauvegarde des données : {e}")

# Utilisation de la fonction
consume_and_save_to_csv(
    topic='surface_water_data',
    output_file='data/output_data',  # Chemin de sortie sous Windows
    bootstrap_servers='localhost:9092',
    batch_size=1000  # Réduire la taille pour des tests rapides
)
