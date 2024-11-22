1. Analyse des données
Nous avons commencé ce projet en explorant les trois jeux de données fournis :

LTM_Data_2022_8_1 : Ce fichier contient les données principales de chimie des eaux de surface. Ces données seront reçues en flux continu via Kafka.
Methods_2022_8_1 : Ce fichier contient des informations méthodologiques sur les processus de collecte des données.
Site_Information_2022_8_1 : Ce fichier fournit des détails sur les sites de collecte, tels que leur emplacement et leurs caractéristiques.
Selon les consignes, les deux derniers fichiers ont été chargés localement depuis HDFS. Nous avons utilisé PySpark pour ce chargement, en explorant et nettoyant les données pour garantir leur intégrité avant intégration. Cela nous a permis de nous familiariser avec PySpark, une technologie que nous avons déjà utilisée en cours.

2. Gestion des données en flux avec Kafka
Le fichier LTM_Data_2022_8_1 est envoyé via Kafka en tant que flux continu. La gestion de ce flux a nécessité plusieurs étapes :

Configuration de Kafka :

Mise en place des brokers Kafka et des topics nécessaires pour simuler un flux de données.
Création d’un producer Kafka qui envoie les données par batch de 10 lignes à intervalles réguliers de 10 secondes.
Création d’un consumer Kafka pour lire ces données en flux et les intégrer dans notre pipeline Spark.
Approche batch :

Nous avons choisi d’enregistrer chaque batch reçu en tant que fichier CSV temporaire.
Ces fichiers sont ensuite fusionnés pour produire un fichier agrégé contenant l’ensemble des données.
Cette méthode garantit une trace des batches individuels, permettant un retour en arrière en cas d’erreur ou d’incohérence.
Ce processus nous a demandé de nous familiariser davantage avec Kafka, notamment pour configurer un flux continu stable et pour gérer la consommation des données sans pertes.

3. Traitement et intégration des données
Une fois les données reçues et fusionnées, nous avons utilisé Spark pour réaliser les traitements nécessaires. Notre objectif principal était de créer une table enrichie contenant des informations agrégées par site de collecte. Voici les indicateurs calculés pour chaque site :

Nombre total d’échantillons collectés sur la période 1980-2020.
Concentration moyenne des composants chimiques clés (ex. : soufre, azote).
Variation annuelle moyenne de ces concentrations, pour suivre les tendances.
Type de site (lac ou cours d'eau) et région géographique.
Ces données structurées ont été enregistrées dans un répertoire appelé structured_table, avec une organisation claire et exploitable pour des analyses futures.

Choix de la base de données
Si nous devions choisir une base de données pour ce projet, nous opterions pour une base de données relationnelle comme PostgreSQL. Cette base est particulièrement adaptée pour :

La gestion de données structurées.
La prise en charge des analyses statistiques grâce à des extensions comme PostGIS (utile pour les données géographiques des sites).
La capacité à restaurer des versions antérieures en cas de problème.
Technologies utilisées
Pour mener à bien ce projet, nous avons utilisé :

Kafka pour la gestion des flux de données.
PySpark et Spark Streaming pour le traitement des données et la génération des indicateurs.
HDFS pour le stockage initial des fichiers statiques.
Ce projet a permis de développer nos compétences pratiques dans l’intégration et le traitement de données massives, tout en respectant les contraintes d’un scénario réel.