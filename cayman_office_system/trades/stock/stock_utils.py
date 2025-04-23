import matplotlib.pyplot as plt
import numpy as np

def plot_stock(df, ticker=None, name=None):
    ticker, name = df.iloc[0]['ticker'], df.iloc[0]['name']
    fig, ax1 = plt.subplots(figsize=(10, 6))

    ax1.fill_between(df['date'], df['total_amount'], color='skyblue', alpha=0.4, label='Total Amount', where=np.isfinite(df['total_amount']))    
    bars = ax1.bar(df['date'], df['realization'], color='lightcoral', alpha=0.6, label='Realization')

    for bar in bars:
        height = bar.get_height()
        if height > 0:  # Only label positive bars
            ax1.text(bar.get_x() + bar.get_width() / 2, height, f'{height:.2e}', ha='center', va='bottom', color='black', fontsize=10)

    ax1.plot(df['date'], df['valuation'], color='orange', label='Valuation', linewidth=2)    
    ax1.plot(df['date'], df['pl'], color='gray', label='P/L', linewidth=1)
    ax1.set_ylabel('KRW', color='black')
    plt.xticks(rotation=90, ha='center')

    ax1.grid(True, which='both', linestyle='--', linewidth=0.5, color='gray', alpha=0.6)
    title_suffix = f': {name} ({ticker})' if name and ticker else ''
    fig.suptitle(f'Stock Valuation'+title_suffix, fontsize=16, color='black')
    ax1.legend(loc='upper left')
    plt.tight_layout()

    plt.show()

    return None

