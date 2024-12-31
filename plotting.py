import math
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
from scipy.interpolate import make_interp_spline
from scipy.interpolate import PchipInterpolator
import io
import numpy as np
import pandas as pd

def plot_ratings_to_users(data):    
    # add bullshit interpolated line for funsies
    df = pd.DataFrame(data)
    df['date_watched'] = pd.to_datetime(df['date_watched'])
    df['year_month'] = df['date_watched'].dt.to_period('M')
    monthly_mean = df.groupby('year_month')['rating'].mean().reset_index()
    monthly_mean['year_month'] = monthly_mean['year_month'].dt.to_timestamp()
    
    # Convert these monthly timestamps to numeric for spline interpolation
    # because make_interp_spline wants numeric x values
    x_raw = mdates.date2num(monthly_mean['year_month'])  # numeric date representation
    y_raw = monthly_mean['rating'].values
    
    fig, ax = plt.subplots(figsize=(8, 4))
    # If there's only 1 or 2 months of data, splines won't do much. 
    # So let's handle the case of insufficient data gracefully:
    if len(x_raw) < 3:
        # Just do a basic plot without splines
        ax.plot(monthly_mean['year_month'], monthly_mean['rating'], marker='o', color='red', label='Monthly Average')
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        fig.autofmt_xdate()
        ax.legend()
        buf = io.BytesIO()
        plt.savefig(buf, format='png')
        buf.seek(0)
        plt.close(fig)
        return buf
    
    # create interpolation thing
    pchip = PchipInterpolator(x_raw, y_raw)
    x_smooth = np.linspace(x_raw.min(), x_raw.max(), 50)
    y_smooth = pchip(x_smooth)
    
    # Plot bullshit thing
    ax.plot(mdates.num2date(x_smooth), y_smooth, color='red', linestyle='--', label='_nolegend_')
    
    # prep data for basic scatter plot.
    #I'm doing this second so that auto-framing thing uses these points rather than the line
    data.sort(key=lambda x: x["date_watched"])
    dates  = [item["date_watched"] for item in data]
    ratings = [item["rating"] for item in data]
    owner_ids = [item["user_id"] for item in data]
    unique_ids = sorted(set(owner_ids))
    id_to_index = {uid: idx for idx, uid in enumerate(unique_ids)}
    color_indices = [id_to_index[uid] for uid in owner_ids]
    
    # plot basic scatter plot
    scatter = ax.scatter(dates, ratings, c=color_indices, cmap="viridis")    
    # Format the x-axis as Year-Month
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    fig.autofmt_xdate()

    # return as a fake image file thing
    buf = io.BytesIO()
    plt.tight_layout()
    plt.savefig(buf, format='png')
    buf.seek(0)
    plt.close(fig)
    return buf
    
def monthly_average(data):
    """Create a DataFrame, group by year-month, and compute monthly avg rating."""
    df = pd.DataFrame(data)

    # Ensure 'date_watched' is in datetime format
    # If it's already datetime objects, you can skip this line.
    df['date_watched'] = pd.to_datetime(df['date_watched'])

    # Group by year-month; dt.to_period('M') collapses dates to just 'YYYY-MM'
    df['year_month'] = df['date_watched'].dt.to_period('M')

    # Compute monthly mean rating
    monthly_mean = df.groupby('year_month')['rating'].mean().reset_index()

    # Convert Period('YYYY-MM') back to a timestamp at the start of that month
    monthly_mean['year_month'] = monthly_mean['year_month'].dt.to_timestamp()

    return monthly_mean

def plot_movienights(data):
    dates = [datetime.strptime(row[0], '%Y-%m-%d') for row in data]
    averages = [row[1] for row in data]
    attendance = [row[2] for row in data]

    # 2) Convert 'attendance' to area if you want circumference proportional to attendance
    #    Adjust scale_factor to control how large or small you want the bubbles
    def attendance_to_area(circumference, scale_factor=20.0):
        # area = circumference^2 / (4*pi)
        return (circumference ** 2) / (4.0 * math.pi) * scale_factor

    sizes = [attendance_to_area(a, scale_factor=20.0) for a in attendance]

    # 3) Create the scatter plot
    fig, ax = plt.subplots(figsize=(8, 5))
    scatter_plot = ax.scatter(dates, averages, s=sizes, alpha=0.6, color='blue', edgecolor='black')

    # 4) Format X-axis to show dates nicely
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax.tick_params(axis='x', rotation=45)

    # 5) Add labels/title
    ax.set_xlabel('Date Watched')
    ax.set_ylabel('Average Rating')
    ax.set_title('Movies Scatterplot by Date, Average Rating, and Attendance')

    plt.tight_layout()
    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    buf.seek(0)
    plt.close(fig)
    return buf
    
def plot_favorites(owner_avg_rating, owner_vantage_rating):
    # 1) Sort owners by largest positive differential
    all_owners = sorted(owner_avg_rating.keys())
    combined_data = []
    for o in owner_avg_rating:
        avg_r = owner_avg_rating[o]
        vant_r = owner_vantage_rating.get(o, 0)
        diff = vant_r - avg_r  # vantage rating - average rating
        combined_data.append((o, avg_r, vant_r, diff))
    combined_data.sort(key=lambda x: x[3], reverse=True)
    all_owners = [x[0] for x in combined_data]
    avg_ratings = [x[1] for x in combined_data]
    vantage_ratings = [x[2] for x in combined_data]
    
    x = np.arange(len(all_owners)) 
    
    # owner average rating received
    fig, ax = plt.subplots(figsize=(10, 6))
    width_main = 0.6
    bar_main = ax.bar(x, avg_ratings, width=width_main, color='skyblue', label='Avg Rating Received')

    # ratings given to movie owners by specific user
    width_overlay = 0.3
    bar_overlay = ax.bar(
        x, 
        vantage_ratings,
        width=width_overlay, 
        color='darkblue',    # stands out from skyblue
        alpha=0.8, 
        label='Vantage User Rating'
    )

    # 7) Labeling
    ax.set_xticks(x)
    ax.set_xticklabels(all_owners, rotation=45, ha='right')  # rotate for readability

    # 8) Adjust y-limit if needed
    ax.set_ylim(0, 10)  # if you know ratings never exceed 10
    # or let autoscale do its job if you prefer

    plt.tight_layout()
    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    buf.seek(0)
    plt.close(fig)
    return buf