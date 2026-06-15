

# Found two scenes with overlap over landfast ice
# New file
zip_1 = '/dmidata/projects/asip-cms/cgf/20180627T120051/S1B_EW_GRDM_1SDH_20180627T205709_20180627T205809_011564_01541A_744C.SAFE.zip'

# Base file
zip_2 = '/dmidata/projects/asip-cms/cgf/20180627T120051/S1A_EW_GRDM_1SDH_20180627T120051_20180627T120155_022542_02711A_75D9.SAFE.zip'


#nc file
nc=
####Plot
skip = 4  # skipping some pixels to plot faster

fig, ax = plt.subplots(1, 3, figsize=(18, 8))

# =========================
# SCENE 2 (reference grid)
# =========================

ax[0].imshow(
    HH_2_clean[::skip, ::skip],
    vmin=np.nanpercentile(HH_2_norm, 2),
    vmax=np.nanpercentile(HH_2_norm, 98),
    cmap='gist_gray'
)

ax[0].set_title("HH Scene 2 - base file 27/06/2018 12:00:51")

# =========================
# RESAMPLED SCENE 1
# =========================

ax[1].imshow(
    HH_1_clean[::skip, ::skip],
    vmin=np.nanpercentile(HH_1_resampled, 2),
    vmax=np.nanpercentile(HH_1_resampled, 98),
    cmap='gist_gray'
)

ax[1].set_title("HH Scene 1 Resampled 27/06/2018 20:57:09")

# =========================
# FAST ICE MASK
# =========================

cmap = plt.cm.gray.copy()
cmap.set_bad(color='red')  # o 'yellow', 'blue', etc.

ax[2].imshow(
    mask_clean[::skip, ::skip],
    cmap=cmap,
    vmin=0,
    vmax=1
)

ax[2].set_title("Ice chart 27/06/2018 12:00:51")

plt.tight_layout()
plt.show()



skip = 4
fig, ax = plt.subplots(1, 3, figsize=(12, 6))
ax[0].imshow(scene2[0,::skip, ::skip], cmap='gist_gray', vmin=np.nanpercentile(HH2, 2), vmax=np.nanpercentile(HH2, 98))
ax[0].set_title("HH Scene 2 - base file \n 27/06/2018 12:00:51")
ax[1].imshow(scene1[0,::skip, ::skip], cmap='gist_gray', vmin=np.nanpercentile(HH1, 2), vmax=np.nanpercentile(HH1, 98))
ax[1].set_title("HH Scene 1 Resampled \n 27/06/2018 20:57:09")
cmap = plt.cm.gray.copy()
cmap.set_bad(color='red')  # o 'yellow', 'blue', etc.
ax[2].imshow(
    ice_chart[::skip, ::skip],
    cmap=cmap,
    vmin=0,
    vmax=1
)
ax[2].set_title("Ice chart 27/06/2018 12:00:51")


fig, ax = plt.subplots(1, n, figsize=(4*n, 6))

for idx, patch_id in enumerate(valid_indices):

    patch = scene1_patches[patch_id][0].cpu().numpy()

    ax[idx].set_title(f"{patch_id}")

    ax[idx].imshow(
        patch,
        cmap='gist_gray',
        vmin=np.nanpercentile(patch, 2),
        vmax=np.nanpercentile(patch, 98)
    )

    ax[idx].axis("off")

plt.tight_layout()
plt.show()