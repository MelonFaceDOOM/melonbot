import math
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
from scipy.interpolate import make_interp_spline
from scipy.interpolate import PchipInterpolator
import io
import numpy as np
import pandas as pd
import seaborn as sns
from scipy.stats import pearsonr

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

    def attendance_to_area(circumference, scale_factor=20.0):
        return (circumference ** 2) / (4.0 * math.pi) * scale_factor

    sizes = [attendance_to_area(a, scale_factor=20.0) for a in attendance]

    fig, ax = plt.subplots(figsize=(8, 5))
    scatter_plot = ax.scatter(dates, averages, s=sizes, alpha=0.6, color='blue', edgecolor='black')

    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax.tick_params(axis='x', rotation=45)

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

def plot_user_similarity(data, min_common=5):
    """
    Plot a clustered heatmap of user rating similarities.
    
    Args:
        data: List of dicts with user_id, movie_id, rating
        min_common: Minimum number of movies in common to calculate correlation
        
    Returns:
        io.BytesIO with PNG plot
    """
    # Convert to DataFrame for easier manipulation
    df = pd.DataFrame(data)
    
    # Pivot to user x movie matrix
    ratings_matrix = df.pivot(index='user_id', columns='movie_id', values='rating')
    
    # Calculate correlation matrix
    n_users = len(ratings_matrix.index)
    if n_users < 2:
        raise ValueError("Need at least 2 users to generate similarity plot.")
        
    corr_matrix = np.zeros((n_users, n_users))
    user_ids = ratings_matrix.index.values
    
    # Track which users have enough movies in common and varying ratings
    valid_users = set()
    
    # First check which users have varying ratings
    users_with_variance = set()
    for i in range(n_users):
        user_ratings = ratings_matrix.iloc[i].dropna()
        if len(user_ratings.unique()) > 1:
            users_with_variance.add(i)
    
    if len(users_with_variance) < 2:
        raise ValueError("Need at least 2 users with varying ratings to calculate correlations.")
    
    for i in range(n_users):
        if i not in users_with_variance:
            continue
        user1_ratings = ratings_matrix.iloc[i]
        
        for j in range(n_users):
            if j not in users_with_variance:
                continue
                
            user2_ratings = ratings_matrix.iloc[j]
            common_mask = user1_ratings.notna() & user2_ratings.notna()
            common_count = sum(common_mask)
            
            if common_count >= min_common:
                # Get the common ratings
                u1_common = user1_ratings[common_mask]
                u2_common = user2_ratings[common_mask]
                
                # Double check for variance in the common ratings
                if len(u1_common.unique()) > 1 and len(u2_common.unique()) > 1:
                    try:
                        corr, _ = pearsonr(u1_common, u2_common)
                        if not np.isnan(corr):
                            corr_matrix[i,j] = corr
                            valid_users.add(i)
                            valid_users.add(j)
                    except Exception:
                        corr_matrix[i,j] = np.nan
                else:
                    corr_matrix[i,j] = np.nan
            else:
                corr_matrix[i,j] = np.nan
    
    # Check if we have enough valid users
    if len(valid_users) < 2:
        raise ValueError(f"Not enough users have {min_common} or more movies in common with varying ratings. Try lowering min_common.")
        
    # Filter to only include users with enough movies in common
    valid_indices = sorted(list(valid_users))
    filtered_matrix = corr_matrix[np.ix_(valid_indices, valid_indices)]
    filtered_user_ids = user_ids[valid_indices]
    
    # Create DataFrame for seaborn
    corr_df = pd.DataFrame(
        filtered_matrix, 
        index=filtered_user_ids,
        columns=filtered_user_ids
    )
    
    # Plot clustered heatmap
    plt.figure(figsize=(10, 8))
    g = sns.clustermap(
        corr_df,
        cmap='RdBu_r',
        center=0,
        vmin=-1,
        vmax=1,
        row_cluster=True,
        col_cluster=True,
        dendrogram_ratio=(.1, .2),
        cbar_pos=(.02, .32, .03, .2)
    )
    
    plt.title('User Rating Similarity (Pearson Correlation)')
    
    # Save to buffer
    buf = io.BytesIO()
    plt.savefig(buf, format='png', bbox_inches='tight')
    buf.seek(0)
    plt.close()
    return buf

def plot_movie_spread(movie_data, ratings_data):
    """
    Plot the distribution of ratings for a movie using strip and violin plots.
    
    Args:
        movie_data: Dict with movie info including title
        ratings_data: List of dicts with rating info for the movie
        
    Returns:
        io.BytesIO with PNG plot
    """
    if not ratings_data:
        raise ValueError("No ratings found for this movie")
        
    # Convert to DataFrame
    df = pd.DataFrame(ratings_data)
    
    # Create figure with two subplots
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(8, 8), height_ratios=[1, 2])
    fig.suptitle(f"Rating Distribution: {movie_data['title']}", y=0.95)
    
    # Strip plot
    sns.stripplot(
        data=df,
        y='rating',
        ax=ax1,
        size=8,
        jitter=0.2,
        alpha=0.6
    )
    ax1.set_xlabel('')
    
    # Violin plot
    sns.violinplot(
        data=df,
        y='rating',
        ax=ax2,
        inner='box'
    )
    
    # Set consistent y-axis limits
    ax1.set_ylim(0, 10)
    ax2.set_ylim(0, 10)
    
    # Add statistics as text
    stats_text = (
        f"Mean: {df['rating'].mean():.2f}\n"
        f"Median: {df['rating'].median():.2f}\n"
        f"Std Dev: {df['rating'].std():.2f}\n"
        f"Count: {len(df)}"
    )
    plt.figtext(0.95, 0.5, stats_text, 
                bbox=dict(facecolor='white', alpha=0.8),
                verticalalignment='center')
    
    # Adjust layout
    plt.tight_layout()
    
    # Save to buffer
    buf = io.BytesIO()
    plt.savefig(buf, format='png', bbox_inches='tight')
    buf.seek(0)
    plt.close()
    return buf

def plot_user_similarity_test():
    """
    Generate synthetic rating data and test the similarity plotting.
    Returns a PNG buffer with the plot.
    """
    # Generate synthetic data
    np.random.seed(42)  # For reproducibility
    
    n_users = 10
    n_movies = 20
    
    # Create user IDs
    user_ids = list(range(1001, 1001 + n_users))
    
    # Create movie IDs
    movie_ids = list(range(1, 1 + n_movies))
    
    # Generate synthetic ratings data
    # Each user will rate movies with their own bias and some random noise
    data = []
    for user_id in user_ids:
        # User's rating bias (between 3 and 8)
        user_bias = np.random.uniform(3, 8)
        
        # Rate a random number of movies (between 5 and n_movies)
        n_ratings = np.random.randint(5, n_movies + 1)
        rated_movies = np.random.choice(movie_ids, n_ratings, replace=False)
        
        for movie_id in rated_movies:
            # Generate rating with bias and noise
            rating = min(10, max(1, user_bias + np.random.normal(0, 1)))
            data.append({
                'user_id': user_id,
                'movie_id': movie_id,
                'rating': rating
            })
    
    # Convert to DataFrame for verification
    df = pd.DataFrame(data)
    print("\nSynthetic Data Summary:")
    print(f"Number of users: {len(df['user_id'].unique())}")
    print(f"Number of movies: {len(df['movie_id'].unique())}")
    print(f"Total ratings: {len(df)}")
    print("\nRatings per user:")
    print(df.groupby('user_id').size())
    print("\nRating distribution:")
    print(df['rating'].describe())
    
    # Plot using the existing function
    return plot_user_similarity(data, min_common=3)