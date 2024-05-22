from database.custom_indicators import SuperTrend, volume_anomality
from ta.volatility import BollingerBands
from ta.trend import EMAIndicator, SMAIndicator, ichimoku_base_line, ichimoku_conversion_line, ichimoku_a, ichimoku_b
from ta.momentum import StochRSIIndicator, WilliamsRIndicator
import pandas as pd
import warnings

# Filtrer les avertissements pour les ignorer
warnings.filterwarnings("ignore")


'''
Ici sont stockées toutes les stratégies qui seront utilisées dans le backtest et le trading automatisé. Chaque stratégie 
est implémentée dans une classe distincte. Pour en ajouter de nouvelles, il suffit de créer une nouvelle classe avec les mêmes
méthodes que celles existantes. Elles prennent le df en entrée qui sera donné lors du backtest ou trading puis génère les 
signaux et les colonnes buy et sell signal. Un nouveau df sera retourné avec les nouvelles colonnes. Ce qui permettra 
d'acheter et de vendre selon la stratégie.
'''


class BollingerVolatility:
    def __init__(self, df):
        self.df = df
      

    def calculate_indicators(self):
        bb = BollingerBands(close=self.df['close'], window=50, window_dev=2)
        super_trend = SuperTrend(self.df["high"], self.df["low"], self.df["close"], atr_window=15, atr_multi=6)
        short_ema = EMAIndicator(close=self.df["close"], window=5).ema_indicator()
        long_ema = EMAIndicator(close=self.df["close"], window=50).ema_indicator()
        # big_ema = EMAIndicator(close=self.df["close"], window=80).ema_indicator()
        # macd = MACD(close=self.df['close'], window_slow=26, window_fast=12, window_sign=9)
        
        # Obtenez les bandes supérieure et inférieure
        upper_band = bb.bollinger_hband()
        lower_band = bb.bollinger_lband()
        
        # Calculez la largeur des bandes
        band_width = upper_band - lower_band
        
        # Comparez la largeur actuelle des bandes avec la moyenne mobile
        self.df['bandes_ecartees'] = (band_width > band_width.rolling(window=14).mean()) & (band_width.shift(1) <= band_width.rolling(window=14).mean().shift(1))
        self.df['super_trend_direction'] = super_trend.super_trend_direction()
        self.df['short_ema'] = short_ema
        self.df['long_ema'] = long_ema
        # self.df['big_ema'] = big_ema

        # Calculer le MACD à l'intérieur de cette fonction
        # self.df['macd'] = macd.macd()
        # self.df['macdsignal'] = macd.macd_signal()

        # Calculer les signaux d'achat et de vente
        condition_achat = (self.df['bandes_ecartees'] == True) & (self.df['super_trend_direction'] == True)# & (self.df['close'] > self.df ['big_ema'])
        condition_vente = (self.df['short_ema'] < self.df['long_ema']) & (self.df['short_ema'].shift(1) >= self.df['long_ema'].shift(1))
        self.df['buy_signal'] = condition_achat
        self.df['sell_signal'] = condition_vente   
        
        return self.df

class BollingerCrossover:
    def __init__(self, df):
        self.df = df
        
        
    def calculate_indicators(self):
        # Calculer les bandes de Bollinger
        indicator_bb = BollingerBands(close=self.df['close'], window=20, window_dev=2)
        super_trend = SuperTrend(self.df["high"], self.df["low"], self.df["close"], atr_window=10, atr_multi=6)
        short_ema = EMAIndicator(close=self.df["close"], window=10).ema_indicator()
        long_ema = EMAIndicator(close=self.df["close"], window=30).ema_indicator()

        # Ajouter les indicateurs au DataFrame des données
        self.df['bb_upper'] = indicator_bb.bollinger_hband()
        self.df['bb_lower'] = indicator_bb.bollinger_lband()
        self.df['super_trend_direction'] = super_trend.super_trend_direction()
        self.df['short_ema'] = short_ema
        self.df['long_ema'] = long_ema

        # Identifier les points d'achat et de vente
        condition_sell = (self.df['short_ema'] < self.df['long_ema']) & (self.df['short_ema'].shift(1) >= self.df['long_ema'].shift(1))
        condition_achat = (self.df['close'] > self.df['bb_lower']) & (self.df['close'].shift(1) <= self.df['bb_lower'].shift(1)) & (self.df['super_trend_direction'] == True)
        
        # Ajouter une colonne pour stocker le signal d'achat et de vente
        self.df['buy_signal'] = condition_achat
        self.df['sell_signal'] = condition_sell
        
        return self.df
    
class AlligatorStrategy:
    def __init__(self, df):
        self.df = df


    def calculate_indicators(self):
        # Calculer les indicateurs
        self.df['EMA_7'] = EMAIndicator(close=self.df['close'], window=7).ema_indicator()
        self.df['EMA_30'] = EMAIndicator(close=self.df['close'], window=30).ema_indicator()
        self.df['EMA_50'] = EMAIndicator(close=self.df['close'], window=50).ema_indicator()
        self.df['EMA_100'] = EMAIndicator(close=self.df['close'], window=100).ema_indicator()
        self.df['EMA_150'] = EMAIndicator(close=self.df['close'], window=150).ema_indicator()
        self.df['EMA_200'] = EMAIndicator(close=self.df['close'], window=200).ema_indicator()
        
        self.df['Stoch_RSI'] = StochRSIIndicator(close=self.df['close'], window=14, smooth1=3, smooth2=3).stochrsi()

        
        condition_achat = (
            (self.df['EMA_7'] > self.df['EMA_30']) &
            (self.df['EMA_30'] > self.df['EMA_50']) &
            (self.df['EMA_50'] > self.df['EMA_100']) &
            (self.df['EMA_100'] > self.df['EMA_150']) &
            (self.df['EMA_150'] > self.df['EMA_200']) &
            (self.df['Stoch_RSI'] < 0.8)
        )
        
        condition_vente = (self.df['EMA_200'] > self.df['EMA_7']) & (self.df['Stoch_RSI'] > 0.2)
        
        self.df['buy_signal'] = condition_achat
        self.df['sell_signal'] = condition_vente
        
        return self.df
    
class TSuperTrendStrategy:
    def __init__(self, df):
        self.df = df
        
    def calculate_indicators(self):
        # Calcul de la première supertrend
        super_trend_values_1 = SuperTrend(self.df["high"], self.df["low"], self.df["close"], atr_window=10, atr_multi=6)
        super_trend_1 = pd.Series(super_trend_values_1).astype(bool).astype(int) * 2 - 1

        # Calcul de la deuxième supertrend avec des paramètres différents
        super_trend_values_2 = SuperTrend(self.df["high"], self.df["low"], self.df["close"], atr_window=15, atr_multi=7)
        super_trend_2 = pd.Series(super_trend_values_2).astype(bool).astype(int) * 2 - 1

        # Calcul de la troisième supertrend avec des paramètres différents
        super_trend_values_3 = SuperTrend(self.df["high"], self.df["low"], self.df["close"], atr_window=20, atr_multi=8)
        super_trend_3 = pd.Series(super_trend_values_3).astype(bool).astype(int) * 2 - 1

        # Ajout des indicateurs au df
        self.df["super_trend_1"] = super_trend_1
        self.df["super_trend_2"] = super_trend_2
        self.df["super_trend_3"] = super_trend_3
        self.df['Stoch_RSI'] = StochRSIIndicator(close=self.df['close'], window=14, smooth1=3, smooth2=3).stochrsi()
        self.df['EMA_90'] = EMAIndicator(close=self.df['close'], window=90).ema_indicator()
        
        condition_achat = (self.df['super_trend_1'] + self.df['super_trend_2'] + self.df['super_trend_3'] >= 1) & (self.df['Stoch_RSI'] < 0.8) & (self.df['EMA_90'] < self.df['close'])
        condition_vente = (self.df['super_trend_1'] + self.df['super_trend_2'] + self.df['super_trend_3'] <= 1) & (self.df['Stoch_RSI'] > 0.2)
        
        self.df['buy_signal'] = condition_achat
        self.df['sell_signal'] = condition_vente
        
        return self.df

class CrossEMAStochRSI:
    def __init__(self, df):
        self.df = df

    def calculate_indicators(self):
        self.df['EMA_28'] = EMAIndicator(close=self.df['close'], window=28).ema_indicator()
        self.df['EMA_48'] = EMAIndicator(close=self.df['close'], window=48).ema_indicator()
        self.df['Stoch_RSI'] = StochRSIIndicator(close=self.df['close'], window=14, smooth1=3, smooth2=3).stochrsi()
        
        
        condition_achat = (self.df['EMA_28'].shift(1) > self.df['EMA_48'].shift(1)) & (self.df['Stoch_RSI'].shift(1) < 0.8)
        condition_vente = (self.df['EMA_28'].shift(1) < self.df['EMA_48'].shift(1)) & (self.df['Stoch_RSI'].shift(1) > 0.2)
        
        self.df['buy_signal'] = condition_achat
        self.df['sell_signal'] = condition_vente
        
        return self.df
    
class TrixStrategy:
    def __init__(self, df):
        self.df = df

    def calculate_indicators(self):
        trixLength = 9
        trixSignal = 21

        # Calcul de l'EMA200
        self.df['EMA200'] = EMAIndicator(close=self.df['close'], window=200).ema_indicator()

        # Calcul de TRIX
        trix_values = EMAIndicator(
            EMAIndicator(
                EMAIndicator(close=self.df['close'], window=trixLength),
                window=trixLength
            ),
            window=trixLength
        )
        self.df['trix'] = trix_values
        self.df['trix_pct'] = trix_values.pct_change() * 100

        # Calcul de TRIX SIGNAL
        trix_signal_values = SMAIndicator(self.df['trix_pct'], trixSignal)
        self.df['trix_signal'] = trix_signal_values > 0

        # Calcul de Stochastic RSI
        self.df['STOCH_RSI'] = StochRSIIndicator(close=self.df['close'], window=14, smooth1=3, smooth2=3).stochrsi()

        # Calcul de TRIX_HISTO
        self.df['TRIX_HISTO'] = self.df['trix_pct'] - self.df['trix_signal']

        # Conditions d'achat et de vente
        condition_achat = (self.df['TRIX_HISTO'] > 0) & (self.df['STOCH_RSI'] < 0.8)
        condition_vente = (self.df['TRIX_HISTO'] < 0) & (self.df['STOCH_RSI'] > 0.2)
        
        self.df['buy_signal'] = condition_achat
        self.df['sell_signal'] = condition_vente

        return self.df

class ichiCloudStochRSI:
    def __init__(self, df):
        self.df = df
        
    def calculate_indicators(self):
        self.df['EMA_50'] = EMAIndicator(close=self.df['close'], window=50).ema_indicator()
        self.df['Stoch_RSI'] = StochRSIIndicator(close=self.df['close'], window=14, smooth1=3, smooth2=3).stochrsi()
        
        # Calcul des indicateurs Ichimoku Cloud
        self.df['KIJUN'] = ichimoku_base_line(self.df['high'], self.df['low'])
        self.df['TENKAN'] = ichimoku_conversion_line(self.df['high'], self.df['low'])
        self.df['SSA'] = ichimoku_a(self.df['high'], self.df['low'], 3, 38).shift(periods=48)
        self.df['SSB'] = ichimoku_b(self.df['high'], self.df['low'], 38, 46).shift(periods=48)
        
        condition_achat = (self.df['close'] > self.df['SSA']) & (self.df['close'] > self.df['SSB']) & (self.df['Stoch_RSI'] < 0.8) & (self.df['EMA_50'] < self.df['close'])
        condition_vente = ((self.df['close'] < self.df['SSA']) | (self.df['close'] < self.df['SSB'])) & (self.df['Stoch_RSI'] > 0.2)
        
        self.df['buy_signal'] = condition_achat
        self.df['sell_signal'] = condition_vente        
        
        return self.df
    
class VolumeAnomaly:
    def __init__(self, df):
        self.df = df

    def calculate_indicators(self):
        # -- Indicator variable --
        volume_window = 10
        willWindow = 14
        self.df['VOL_ANO'] = volume_anomality(self.df, volume_window)
        self.df['MIN20'] = self.df['close'].rolling(20).min()
        self.df['WillR'] = WilliamsRIndicator(high=self.df['high'], low=self.df['low'], close=self.df['close'], lbp=willWindow).williams_r()
        self.df['CANDLE_DIFF'] = abs(self.df['open'] - self.df['close'])
        self.df['MEAN_DIFF'] = self.df['CANDLE_DIFF'].rolling(10).mean()
        
        willROverBought = -20
        
        # Utiliser .shift(1) pour obtenir les valeurs de la ligne précédente
        previous_row = self.df.shift(1)
        
        # Conditions d'achat basées sur la ligne actuelle et la ligne précédente
        condition_achat = (
            (self.df['VOL_ANO'] > 0) &
            (previous_row['VOL_ANO'] < 0) &
            (previous_row['close'] <= previous_row['MIN20']) &
            (previous_row['CANDLE_DIFF'] > self.df['CANDLE_DIFF']) &
            ((previous_row['open'] - previous_row['close']) > 0.0025 * self.df['close'])
        )
        
        condition_vente = self.df['WillR'] > willROverBought
        
        
        self.df['buy_signal'] = condition_achat
        self.df['sell_signal'] = condition_vente
        
        return self.df
    
    
