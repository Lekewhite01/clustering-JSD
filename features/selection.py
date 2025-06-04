import numpy as np
import pandas as pd
from sklearn.feature_selection import VarianceThreshold
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from itertools import combinations
import warnings
warnings.filterwarnings('ignore')

def select_features_for_clustering(X, max_features=None, variance_threshold=0.01, 
                                 correlation_threshold=0.9, n_clusters=3, 
                                 scoring_method='silhouette', random_state=42):
    """
    Automatically select features for clustering using multiple criteria.
    
    Parameters:
    -----------
    X : pandas.DataFrame or numpy.array
        Input features
    max_features : int, optional
        Maximum number of features to select (default: None)
    variance_threshold : float, default=0.01
        Minimum variance threshold for feature selection
    correlation_threshold : float, default=0.9
        Maximum correlation threshold between features
    n_clusters : int, default=3
        Number of clusters for evaluation
    scoring_method : str, default='silhouette'
        Method to evaluate clustering quality ('silhouette' or 'inertia')
    random_state : int, default=42
        Random state for reproducibility
    
    Returns:
    --------
    dict : Dictionary containing selected features and selection details
    """
    
    # Convert to DataFrame if numpy array
    if isinstance(X, np.ndarray):
        X = pd.DataFrame(X, columns=[f'feature_{i}' for i in range(X.shape[1])])
    
    original_features = list(X.columns)
    print(f"Starting with {len(original_features)} features")
    
    # Step 1: Remove low variance features
    selector = VarianceThreshold(threshold=variance_threshold)
    X_var = pd.DataFrame(
        selector.fit_transform(X), 
        columns=X.columns[selector.get_support()]
    )
    print(f"After variance filtering: {X_var.shape[1]} features")
    
    # Step 2: Remove highly correlated features
    if X_var.shape[1] > 1:
        corr_matrix = X_var.corr().abs()
        upper_triangle = corr_matrix.where(
            np.triu(np.ones(corr_matrix.shape), k=1).astype(bool)
        )
        
        # Find features with correlation above threshold
        high_corr_features = [
            column for column in upper_triangle.columns 
            if any(upper_triangle[column] > correlation_threshold)
        ]
        
        X_corr = X_var.drop(columns=high_corr_features)
        print(f"After correlation filtering: {X_corr.shape[1]} features")
    else:
        X_corr = X_var
    
    # Step 3: If still too many features, use PCA or feature selection
    if max_features and X_corr.shape[1] > max_features:
        if max_features >= X_corr.shape[1] * 0.5:
            # Use iterative feature selection for moderate reduction
            selected_features = _iterative_feature_selection(
                X_corr, max_features, n_clusters, scoring_method, random_state
            )
            X_final = X_corr[selected_features]
        else:
            # Use PCA for significant dimensionality reduction
            scaler = StandardScaler()
            X_scaled = scaler.fit_transform(X_corr)
            
            pca = PCA(n_components=max_features, random_state=random_state)
            X_pca = pca.fit_transform(X_scaled)
            
            X_final = pd.DataFrame(
                X_pca, 
                columns=[f'PC_{i+1}' for i in range(max_features)]
            )
            print(f"Applied PCA: {X_final.shape[1]} components")
    else:
        X_final = X_corr
    
    # Step 4: Evaluate final feature set
    if X_final.shape[1] > 1:
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X_final)
        
        kmeans = KMeans(n_clusters=n_clusters, random_state=random_state, n_init=10)
        labels = kmeans.fit_predict(X_scaled)
        
        if scoring_method == 'silhouette':
            score = silhouette_score(X_scaled, labels)
            print(f"Final silhouette score: {score:.3f}")
        else:
            score = kmeans.inertia_
            print(f"Final inertia: {score:.3f}")
    else:
        score = None
        print("Warning: Only one feature remaining")
    
    return {
        'selected_features': list(X_final.columns),
        'n_selected': X_final.shape[1],
        'clustering_score': score,
        'feature_data': X_final,
        'selection_summary': {
            'original_features': len(original_features),
            'after_variance_filter': X_var.shape[1],
            'after_correlation_filter': X_corr.shape[1],
            'final_features': X_final.shape[1]
        }
    }

def _iterative_feature_selection(X, max_features, n_clusters, scoring_method, random_state):
    """Helper function for iterative feature selection based on clustering quality."""
    
    scaler = StandardScaler()
    features = list(X.columns)
    best_score = -np.inf if scoring_method == 'silhouette' else np.inf
    best_features = []
    
    # Start with all features and remove worst performers
    current_features = features.copy()
    
    while len(current_features) > max_features:
        scores = []
        
        # Test removing each feature
        for feature in current_features:
            test_features = [f for f in current_features if f != feature]
            X_test = X[test_features]
            
            if len(test_features) > 1:
                X_scaled = scaler.fit_transform(X_test)
                kmeans = KMeans(n_clusters=n_clusters, random_state=random_state, n_init=10)
                labels = kmeans.fit_predict(X_scaled)
                
                if scoring_method == 'silhouette':
                    score = silhouette_score(X_scaled, labels)
                else:
                    score = kmeans.inertia_
                
                scores.append((feature, score))
        
        # Remove the feature whose removal gives the best score
        if scoring_method == 'silhouette':
            worst_feature = max(scores, key=lambda x: x[1])[0]
        else:
            worst_feature = min(scores, key=lambda x: x[1])[0]
        
        current_features.remove(worst_feature)
    
    return current_features


# Example usage:
if __name__ == "__main__":
    # Create sample data
    np.random.seed(42)
    n_samples, n_features = 200, 15
    
    # Generate correlated features and noise
    X_base = np.random.randn(n_samples, 5)
    X_noise = np.random.randn(n_samples, 5) * 0.1
    X_corr = X_base + np.random.randn(n_samples, 5) * 0.3  # Correlated features
    
    X_sample = np.column_stack([X_base, X_corr, X_noise])
    feature_names = [f'feature_{i}' for i in range(n_features)]
    
    df = pd.DataFrame(X_sample, columns=feature_names)
    
    # Apply feature selection
    result = select_features_for_clustering(
        df, 
        max_features=8,
        n_clusters=3,
        scoring_method='silhouette'
    )
    
    print(f"\nSelected features: {result['selected_features']}")
    print(f"Selection summary: {result['selection_summary']}")