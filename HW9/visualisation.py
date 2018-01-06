import datetime
import io
from collections import Counter
import matplotlib
matplotlib.use('Agg')
from matplotlib import pyplot as plt
import numpy as np


def items_stats(items):
    date_counts = Counter()
    for item in items:
        date = datetime.datetime.fromtimestamp(item["date"]).strftime("%Y-%m-%d")
        date_counts[date] += 1
    return [(k, date_counts[k]) for k in sorted(date_counts)]


def plot_hist_buffer(hist_data):
    y_pos = np.arange(len(hist_data))
    y = [x[1] for x in hist_data]

    plt.figure(figsize=(20, 5))
    plt.bar(y_pos, y, align='center', alpha=0.5)
    plt.xticks(y_pos, [x[0] for x in hist_data], rotation='vertical')
    plt.ylabel('Post count')
    plt.title('Post count by date (last 100 posts)')

    buf = io.BytesIO()
    buf.name = 'stats.png'
    plt.savefig(buf, format='png', bbox_inches='tight')
    buf.seek(0)
    return buf
