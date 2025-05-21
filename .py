import numpy as np
import matplotlib.pyplot as plt
from sklearn.datasets import make_regression
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error

# Paramètres initiaux
n_observations = 100
n_features = 1
bruit_values = np.arange(10, 41, 5)  # Bruit de 10 à 40 (pas de 5)
mse_values = []

# Configuration des subplots
plt.figure(figsize=(15, 10))

# Boucle sur les valeurs de bruit
for i, bruit in enumerate(bruit_values):
    # Génération des données
    X, y = make_regression(n_samples=n_observations, n_features=n_features, noise=bruit, random_state=42)
    
    # Entraînement du modèle
    model = LinearRegression()
    model.fit(X, y)
    y_pred = model.predict(X)
    
    # Calcul du MSE
    mse = mean_squared_error(y, y_pred)
    mse_values.append(mse)
    
    # Affichage des données et de la régression
    plt.subplot(3, 3, i + 1)
    plt.scatter(X, y, color='blue', label='Données', alpha=0.6)
    plt.plot(X, y_pred, color='red', linewidth=2, label='Régression')
    plt.title(f'Bruit = {bruit}\nMSE = {mse:.2f}')
    plt.xlabel('Variable explicative')
    plt.ylabel('Variable à expliquer')
    plt.legend()

plt.tight_layout()
plt.show()

# Courbe MSE en fonction du bruit
plt.figure(figsize=(8, 5))
plt.plot(bruit_values, mse_values, marker='o', linestyle='-', color='green')
plt.title('Variation du MSE en fonction du bruit')
plt.xlabel('Niveau de bruit')
plt.ylabel('MSE')
plt.grid(True)
plt.show()