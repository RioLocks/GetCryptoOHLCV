import os
import asyncio
import json
from datetime import datetime, timezone
import ccxt.async_support as ccxt
from csv import writer as csv_writer


async def timestamp_to_date(timestamp):
    utc_datetime = datetime.fromtimestamp(timestamp / 1000, timezone.utc)
    return utc_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]


async def date_to_timestamp(date_str):
    dt = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S.%f')
    timestamp = int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)
    return timestamp


async def get_last_date(filepath):
    try:
        with open(filepath, 'r') as file:
            lines = file.readlines()
            if lines:
                last_line = lines[-1]
                last_date = last_line.split(',')[0]
                return last_date
            else:
                print(f"Date introuvable dans {filepath}")
                return None  # Retourner None si le fichier est vide
    except FileNotFoundError:
        print(f"Fichier dans le répertoire {filepath} introuvable")
        return None  # Retourner None si le fichier n'existe pas


async def update_data():
    tf_ms = json.load(open(r'database\tf_ms.json'))  # Chargement des données de timeframes
    exchange_limit = json.load(open(r'database\exchange_limit.json'))  # Chargement des limites d'échange

    for exchange_name in exchange_limit:
        exchange_name = "binance"  # Remplacez "Binance" par "binance" si c'est le nom correct de l'échange
        exchange = getattr(ccxt, exchange_name)()
        await exchange.load_markets()

        downloaded_data = False  # Variable de contrôle

        for tf_folder in os.listdir(r"C:\Users\ricar\Desktop\VSCodeProjects\Crypto\database\Binance"):  # Parcourir chaque sous-dossier dans le répertoire "Binance"
            tf = tf_folder.split("_")[-1]  # Extraire le timeframe à partir du nom du sous-dossier
            tf_ms_value = tf_ms[tf]  # Récupérer la valeur de tf_ms pour ce timeframe
            dir_path = f'C:\\Users\\ricar\\Desktop\\VSCodeProjects\\Crypto\\database\\Binance\\{tf_folder}'

            for filename in os.listdir(dir_path):  # Parcourir chaque fichier dans le sous-dossier
                filepath = os.path.join(dir_path, filename)
                if os.path.isfile(filepath):
                    symbol = filename.split(".")[0]  # Extraire le nom de la paire à partir du nom du fichier
                    symbol_with_usdt = f"{symbol.replace('USDT', '/USDT')}"  # Remplacer "USDT" par "/USDT"
                    last_date_str = await get_last_date(filepath)
                    if last_date_str is not None:
                        last_timestamp = await date_to_timestamp(last_date_str)
                        current_timestamp = int(datetime.now().timestamp() * 1000)
                        # Vérifier si les données sont à jour
                        if current_timestamp - last_timestamp > tf_ms_value:
                            await get_ohlcv(exchange, symbol_with_usdt, tf, last_timestamp, exchange_limit[exchange_name], tf_ms_value, filepath)
                            downloaded_data = True  # Données téléchargées
                        else:
                            print(f'Data for {symbol} on {exchange.id} for timeframe {tf} is up to date. Skipping download.')
                    else:
                        print(f'Error: No last date available for {filepath}. Skipping download.')

        # Libérer les ressources de l'échange
        await exchange.close()

        # Vérification s'il y a eu des données téléchargées
        if not downloaded_data:
            print("Aucune nouvelle donnée téléchargée. Arrêt du programme.")
            return  # Arrêt du programme si aucune nouvelle donnée n'a été téléchargée

        # Attendez un peu pour permettre à la boucle d'événements de se terminer proprement
        await asyncio.sleep(1)




async def get_ohlcv(exchange, symbol, timeframe, since_date, limit, tf_ms, filepath):
    try:
        result_ohlcv = []

        since_date = since_date + tf_ms

        ohlcv = await exchange.fetch_ohlcv(symbol, timeframe, since_date, limit)
        for entry in ohlcv:
            entry[0] = await timestamp_to_date(entry[0])
            result_ohlcv.append(entry)

        with open(filepath, 'a', newline='') as file:
            csv_writer_obj = csv_writer(file)
            csv_writer_obj.writerows(result_ohlcv)

        print(f'Data updated for {symbol} on {exchange.id} for timeframe {timeframe}')
    except Exception as e:
        print(f'Error fetching data for {symbol} on {exchange.id} for timeframe {timeframe}: {e}')


asyncio.run(update_data())
