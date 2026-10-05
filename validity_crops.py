from glob import glob
from random import random
import zarr
import numpy as np
import matplotlib.pyplot as plt

patches = glob.glob('/dmidata/projects/asip-cms/cgf/zarr_files2/*/*.zarr')
train_patches = patches[0:50]

# --------------------------------------------------
# 2. Parámetros de la prueba
# --------------------------------------------------

crop_size = 512
n_crops_per_patch = 100

all_valid_ratios = []


# --------------------------------------------------
# 3. Generar crops aleatorios
# --------------------------------------------------

for patch_path in train_patches:

    patch = zarr.open(patch_path, 'r')

    for _ in range(n_crops_per_patch):

        # Coordenadas aleatorias del crop
        x = random.randint(
            0,
            1024 - crop_size
        )

        y = random.randint(
            0,
            1024 - crop_size
        )

        # Solo usamos los 4 canales SAR
        crop = patch.oindex[
            [0, 1, 2, 3],
            x:x + crop_size,
            y:y + crop_size
        ]

        # Un píxel es válido si los 4 canales son válidos
        valid_mask = np.isfinite(crop).all(axis=0)

        # Porcentaje de píxeles válidos
        valid_ratio = valid_mask.mean()

        all_valid_ratios.append(valid_ratio)


# Convertimos a numpy array
all_valid_ratios = np.array(all_valid_ratios)

print("Número total de crops:", len(all_valid_ratios))

print("Valid ratio mínimo:", all_valid_ratios.min())
print("Valid ratio máximo:", all_valid_ratios.max())
print("Valid ratio medio:", all_valid_ratios.mean())
thresholds = [0.50, 0.60, 0.70, 0.80, 0.90]

for threshold in thresholds:

    n_pass = (all_valid_ratios >= threshold).sum()

    percentage = (
        all_valid_ratios >= threshold
    ).mean() * 100

    print(
        f"Umbral {threshold:.2f}: "
        f"{n_pass}/{len(all_valid_ratios)} crops "
        f"({percentage:.1f}%)"
    )
plt.figure(figsize=(8, 5))

plt.hist(
    all_valid_ratios,
    bins=30
)

plt.axvline(
    0.50,
    linestyle='--',
    label='0.50'
)

plt.axvline(
    0.60,
    linestyle='--',
    label='0.60'
)

plt.axvline(
    0.70,
    linestyle='--',
    label='0.70'
)

plt.axvline(
    0.80,
    linestyle='--',
    label='0.80'
)

plt.axvline(
    0.90,
    linestyle='--',
    label='0.90'
)

plt.xlabel("SAR valid ratio")
plt.ylabel("Número de crops")
plt.title("Distribución de validez de los crops")
plt.legend()

plt.show()