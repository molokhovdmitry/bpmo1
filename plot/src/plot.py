import time
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

METRICS_PATH = './logs/metric_log.csv'
PLOT_PATH = './logs/error_distribution.png'

try:
    while True:
        time.sleep(4)
        df = pd.read_csv(METRICS_PATH, index_col=0)
        plot = sns.histplot(df, x='absolute_error', kde=True)
        plt.savefig(PLOT_PATH, format='png', dpi=300)
        plt.clf()

except Exception as e:
    print(e)
    print('Не удалось построить график')
