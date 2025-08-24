import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

""" 
Partir d'un vrai achat : On va prendre une ligne au hasard dans la table order_items. Cette ligne nous donne un product_id et un order_id qui sont réels.
Trouver l'acheteur et la date : En utilisant l'order_id, on va retrouver le customer_unique_id et la date d'achat (order_purchase_timestamp) dans la table orders.
Trouver la campagne pertinente : En utilisant le product_id, on va retrouver la catégorie du produit dans la table products. On cherche ensuite dans notre campaigns.csv s'il y avait une campagne active pour cette catégorie de produit à la date de l'achat.
Générer des événements "autour" de l'achat : Si on a trouvé une campagne pertinente, on va simuler des événements publicitaires pour cet utilisateur et ce produit qui ont eu lieu juste avant la date d'achat. 
Par exemple, une ou deux impressions dans les 3 jours qui précèdent, et peut-être un clic quelques heures avant.
Avantage de cette approche :

Hyper-réaliste : Chaque événement publicitaire est maintenant lié à un achat réel.
Causalité simulée : On crée une chaîne logique : l'utilisateur a vu la pub -> il a cliqué -> il a acheté.
Pas plus complexe : La logique du script reste simple. On ne fait que lire plus de fichiers et faire des recherches simples (des "lookups").
 """

# --- ÉTAPE 1: CONFIGURATION ---
# Assurez-vous que tous ces fichiers CSV sont dans le même dossier que le script,
# ou mettez les chemins complets.
# --- ÉTAPE 1: CONFIGURATION ---
PATH_TO_CUSTOMERS_CSV = './data/olist_customers_dataset.csv'
PATH_TO_PRODUCTS_CSV = './data/olist_products_dataset.csv'
PATH_TO_ORDERS_CSV = './data/olist_orders_dataset.csv'
PATH_TO_ORDER_ITEMS_CSV = './data/olist_order_items_dataset.csv'
PATH_TO_CAMPAIGNS_CSV = './data/campaigns.csv'

OUTPUT_PATH = 'dbt_notlimi_project/seeds/ad_events.csv'

# Paramètres de la simulation
# On va générer des événements pour une fraction des ventes réelles
FRACTION_OF_SALES_TO_SIMULATE = 1.0 # 100%
CLICK_THROUGH_RATE = 0.10 # 10% de chance qu'une impression soit suivie d'un clic

print("--- Démarrage de la simulation V3 (basée sur les ventes réelles) ---")

# --- ÉTAPE 2: CHARGER TOUTES LES DONNÉES DE RÉFÉRENCE ---
try:
    customers_df = pd.read_csv(PATH_TO_CUSTOMERS_CSV)
    products_df = pd.read_csv(PATH_TO_PRODUCTS_CSV)
    orders_df = pd.read_csv(PATH_TO_ORDERS_CSV)
    order_items_df = pd.read_csv(PATH_TO_ORDER_ITEMS_CSV)
    campaigns_df = pd.read_csv(PATH_TO_CAMPAIGNS_CSV)
    print("Fichiers de référence chargés.")
except FileNotFoundError as e:
    print(f"ERREUR: Fichier non trouvé. Vérifiez vos chemins. Détails: {e}")
    exit()

# Conversion des dates en objets datetime
orders_df['order_purchase_timestamp'] = pd.to_datetime(orders_df['order_purchase_timestamp'])
campaigns_df['start_date'] = pd.to_datetime(campaigns_df['start_date'])
campaigns_df['end_date'] = pd.to_datetime(campaigns_df['end_date'])

# --- ÉTAPE 3: PRÉPARER UNE TABLE DE VENTES COMPLÈTE ---
# On joint toutes les informations sur les ventes en une seule table
sales_df = pd.merge(order_items_df, orders_df, on='order_id')
sales_df = pd.merge(sales_df, products_df, on='product_id')
sales_df = pd.merge(sales_df, customers_df, on='customer_id')
print("Table de ventes complète créée.")

# --- ÉTAPE 4: GÉNÉRER LES ÉVÉNEMENTS BASÉS SUR LES VENTES ---
events_data = []
event_id_counter = 1

# On ne prend qu'un échantillon des ventes pour ne pas avoir un fichier trop gros
sales_sample = sales_df.sample(frac=FRACTION_OF_SALES_TO_SIMULATE)
print(f"Génération d'événements pour {len(sales_sample)} ventes...")

for _, sale in sales_sample.iterrows():
    purchase_date = sale['order_purchase_timestamp']
    product_category = sale['product_category_name']
    
    # Trouver une campagne pertinente (bonne catégorie et date valide)
    relevant_campaign = campaigns_df[
        (campaigns_df['product_category'] == product_category) &
        (campaigns_df['start_date'] <= purchase_date) &
        (campaigns_df['end_date'] >= purchase_date)
    ]
    
    # Si on a trouvé au moins une campagne active pour ce produit à ce moment-là
    if not relevant_campaign.empty:
        campaign_info = relevant_campaign.iloc[0] # On prend la première si plusieurs
        
        # Simuler 1 à 3 impressions dans les 3 jours avant l'achat
        for _ in range(random.randint(1, 3)):
            impression_time = purchase_date - timedelta(days=random.uniform(0.5, 3), hours=random.uniform(0, 23))
            events_data.append({
                'event_id': event_id_counter,
                'event_timestamp': impression_time.strftime('%Y-%m-%d %H:%M:%S'),
                'user_unique_id': sale['customer_unique_id'],
                'campaign_id': campaign_info['campaign_id'],
                'product_id': sale['product_id'],
                'event_type': 'impression'
            })
            event_id_counter += 1
            
            # Simuler un clic avec une probabilité de 10% après une impression
            if random.random() < CLICK_THROUGH_RATE:
                click_time = impression_time + timedelta(minutes=random.randint(1, 60))
                # S'assurer que le clic a bien lieu avant l'achat
                if click_time < purchase_date:
                    events_data.append({
                        'event_id': event_id_counter,
                        'event_timestamp': click_time.strftime('%Y-%m-%d %H:%M:%S'),
                        'user_unique_id': sale['customer_unique_id'],
                        'campaign_id': campaign_info['campaign_id'],
                        'product_id': sale['product_id'],
                        'event_type': 'click'
                    })
                    event_id_counter += 1

# --- ÉTAPE 5: CRÉER LE DATAFRAME FINAL ET SAUVEGARDER ---
events_df = pd.DataFrame(events_data)
events_df.to_csv(OUTPUT_PATH, index=False)

print(f"--- Simulation terminée ---")
print(f"Fichier '{OUTPUT_PATH}' créé avec succès avec {len(events_df)} événements.")
print("\nAperçu des données générées :")
print(events_df.head())