# Basic UNet implementation

import torch
import torch.nn as nn
from torchinfo import summary
import zarr
import random
import numpy as np
import glob
import albumentations as A
import wandb
import matplotlib.pyplot as plt
import numpy as np
import os
from tqdm import tqdm
import json
import datetime
from albumentations.pytorch import ToTensorV2

class DoubleConv(nn.Module):
    def __init__(self, in_channels, out_channels):
        super().__init__()

        self.block = nn.Sequential(
            nn.Conv2d(in_channels, out_channels, kernel_size=3, stride=1, padding='same'),
            nn.BatchNorm2d(out_channels),
            nn.ReLU(inplace=True),
            nn.Conv2d(out_channels, out_channels, kernel_size=3, stride=1, padding='same'),
            nn.BatchNorm2d(out_channels),
            nn.ReLU(inplace=True)
        )

    def forward(self, x):
        return self.block(x)

class UNet(nn.Module):
    def __init__(
            self,
            in_channels,
            out_channels,
            dims = [24, 48, 96, 192, 384, 768]
        ):
        super().__init__()

        self.pool = nn.MaxPool2d(kernel_size=2)

        self.enc1 = DoubleConv(in_channels, dims[0])
        self.enc2 = DoubleConv(dims[0], dims[1])
        self.enc3 = DoubleConv(dims[1], dims[2])
        self.enc4 = DoubleConv(dims[2], dims[3])
        self.enc5 = DoubleConv(dims[3], dims[4])
        self.enc6 = DoubleConv(dims[4], dims[5])

        self.dec5 = DoubleConv(dims[5] + dims[4], dims[4])
        self.dec4 = DoubleConv(dims[4] + dims[3], dims[3])
        self.dec3 = DoubleConv(dims[3] + dims[2], dims[2])
        self.dec2 = DoubleConv(dims[2] + dims[1], dims[1])
        self.dec1 = DoubleConv(dims[1] + dims[0], dims[0])

        self.out = nn.Conv2d(dims[0], out_channels, kernel_size=1, stride=1, padding=0, bias=True)

    def forward(self, x):
        x1 = self.enc1(x)
        x2 = self.enc2(self.pool(x1))
        x3 = self.enc3(self.pool(x2))
        x4 = self.enc4(self.pool(x3))
        x5 = self.enc5(self.pool(x4))
        x = self.enc6(self.pool(x5))

        x = self._interpolate_like(x, x5)
        x = self.dec5(torch.cat([x, x5], dim=1))
        x = self._interpolate_like(x, x4)
        x = self.dec4(torch.cat([x, x4], dim=1))
        x = self._interpolate_like(x, x3)
        x = self.dec3(torch.cat([x, x3], dim=1))
        x = self._interpolate_like(x, x2)
        x = self.dec2(torch.cat([x, x2], dim=1))
        x = self._interpolate_like(x, x1)
        x = self.dec1(torch.cat([x, x1], dim=1))

        return self.out(x)

    def _interpolate_like(self, src, tar, mode='bilinear'):
        return torch.nn.functional.interpolate(src, size=tar.shape[2:], mode=mode, align_corners=True)



# UNet init
unet = UNet(
    in_channels=4,
    out_channels=1,
    dims=[16, 32, 64, 128, 256, 512]
)

# Testing forward pass
bs = 4
x = torch.rand(bs, 4, 512, 512)
out = unet(x)
print(out.shape)

# torchinfo summary
summary(unet, [(bs, 4, 512, 512)])


class CustomDataset(torch.utils.data.Dataset):

    def __init__(
        self,
        patch_paths,
        patch_size=1024,
        crop_size=512,
        is_train=True,
        min_valid_ratio=0.70,
        max_crop_attempts=20
    ):
        """
        Dataset for SAR-based fast-ice segmentation.

        Each Zarr patch contains 5 channels:
            0 -> HH2
            1 -> HV2
            2 -> HH1
            3 -> HV1
            4 -> Ice chart (label)

        During training:
            - A random 512x512 crop is selected.
            - The crop must contain at least `min_valid_ratio`
              valid SAR pixels.
            - If the crop is not valid enough, another random
              crop is tried.
            - SAR NaNs are replaced by 0.
            - A SAR validity mask is added as a fifth input channel.

        Therefore, the model receives:
            channel 0 -> HH2
            channel 1 -> HV2
            channel 2 -> HH1
            channel 3 -> HV1
            channel 4 -> SAR validity mask

        During validation:
            - The complete 1024x1024 patch is used.
            - No random crop is performed.
        """

        self.patch_paths = patch_paths

        self.patch_size = patch_size
        self.crop_size = crop_size
        self.half_crop_size = self.crop_size // 2

        self.is_train = is_train

        # Minimum fraction of valid SAR pixels required
        # in a training crop.
        self.min_valid_ratio = min_valid_ratio

        # Maximum number of random crops tested before
        # falling back to the best crop found.
        self.max_crop_attempts = max_crop_attempts

        # ------------------------------------------------------
        # Channel definitions
        # ------------------------------------------------------

        # SAR input channels in the Zarr file
        self.input_dict = {
            'HH2': 0,
            'HV2': 1,
            'HH1': 2,
            'HV1': 3
        }

        # Ice chart label in the Zarr file
        self.labels_dict = {
            'IC': 4
        }

        self.input_indices = list(self.input_dict.values())
        self.label_indices = list(self.labels_dict.values())

        # ------------------------------------------------------
        # Data augmentation / tensor conversion
        # ------------------------------------------------------

        self.transform = (
            self._get_train_transforms()
            if self.is_train
            else self._get_val_transforms()
        )

    def __len__(self):
        """
        Number of available Zarr patches.
        """
        return len(self.patch_paths)

    def __getitem__(self, idx):

        # Open the Zarr patch
        patch = zarr.open(self.patch_paths[idx], 'r')

        # ======================================================
        # TRAINING
        # ======================================================

        if self.is_train:

            # --------------------------------------------------
            # Find a sufficiently valid random crop
            # --------------------------------------------------

            best_crop = None
            best_valid_ratio = -1.0

            for _ in range(self.max_crop_attempts):

                # Select a random 512x512 crop
                x, y = self._random_crop()

                # Read only the four SAR channels
                crop = patch.oindex[
                    self.input_indices,
                    x:x + self.crop_size,
                    y:y + self.crop_size
                ]

                # --------------------------------------------------
                # Determine which pixels are valid in ALL SAR
                # channels.
                #
                # crop shape:
                #     (4, 512, 512)
                #
                # valid_mask shape:
                #     (512, 512)
                # --------------------------------------------------

                valid_mask = np.isfinite(crop).all(axis=0)

                # Fraction of pixels that are valid in all
                # four SAR channels.
                valid_ratio = valid_mask.mean()

                # Keep track of the best crop we have seen.
                if valid_ratio > best_valid_ratio:
                    best_valid_ratio = valid_ratio
                    best_crop = (x, y)

                # If this crop has enough valid pixels,
                # accept it immediately.
                if valid_ratio >= self.min_valid_ratio:
                    break

            # --------------------------------------------------
            # Use the selected crop.
            #
            # If no crop reached 70%, `best_crop` contains the
            # crop with the highest valid-pixel ratio found during
            # the attempts.
            # --------------------------------------------------

            x, y = best_crop

            # Read SAR channels
            inputs = patch.oindex[
                self.input_indices,
                x:x + self.crop_size,
                y:y + self.crop_size
            ]

            # Read ice-chart label
            labels = patch.oindex[
                self.label_indices,
                x:x + self.crop_size,
                y:y + self.crop_size
            ]

            # --------------------------------------------------
            # Change from:
            #
            #     (channels, height, width)
            #
            # to:
            #
            #     (height, width, channels)
            #
            # because this is the format expected by
            # Albumentations.
            # --------------------------------------------------

            inputs = np.transpose(inputs, (1, 2, 0))
            labels = np.transpose(labels, (1, 2, 0))

        # ======================================================
        # VALIDATION
        # ======================================================

        else:

            # Use the complete 1024x1024 patch during validation.

            inputs = patch.oindex[
                self.input_indices
            ]

            labels = patch.oindex[
                self.label_indices
            ]

            # Convert to H x W x C
            inputs = np.transpose(inputs, (1, 2, 0))
            labels = np.transpose(labels, (1, 2, 0))

        # ======================================================
        # CREATE SAR VALIDITY MASK
        # ======================================================

        # The mask is based ONLY on the SAR inputs.
        #
        # True  -> this pixel contains valid SAR information
        # False -> at least one SAR channel is NaN
        #
        # This mask does NOT use the ice chart.
        valid_mask = np.isfinite(inputs).all(axis=2)

        # ======================================================
        # REPLACE SAR NaNs
        # ======================================================

        # Neural networks cannot safely process NaNs because
        # convolutions can propagate NaNs through the network.
        #
        # We therefore replace invalid SAR values with 0.
        #
        # The validity mask below tells the network that these
        # zeros correspond to missing/invalid SAR information.
        inputs = np.nan_to_num(
            inputs,
            nan=0.0
        )

        # ======================================================
        # ADD VALIDITY MASK AS FIFTH INPUT CHANNEL
        # ======================================================

        # valid_mask currently has shape:
        #
        #     (H, W)
        #
        # Add a channel dimension:
        #
        #     (H, W, 1)
        #
        # and concatenate it with the four SAR channels.
        valid_mask = valid_mask.astype(np.float32)

        inputs = np.concatenate(
            [
                inputs,
                valid_mask[..., None]
            ],
            axis=2
        )

        # At this point:
        #
        # inputs.shape =
        #
        #     (512, 512, 5)    training
        #
        # or
        #
        #     (1024, 1024, 5)  validation
        #
        # where the five channels are:
        #
        #     0 -> HH2
        #     1 -> HV2
        #     2 -> HH1
        #     3 -> HV1
        #     4 -> SAR validity mask

        # ======================================================
        # AUGMENTATION + CONVERSION TO TORCH
        # ======================================================

        augmented = self.transform(
            image=inputs,
            mask=labels
        )

        inputs = augmented['image']
        labels = augmented['mask']

        # Final shapes:
        #
        # Training:
        #     inputs -> (5, 512, 512)
        #     labels -> (1, 512, 512)
        #
        # Validation:
        #     inputs -> (5, 1024, 1024)
        #     labels -> (1, 1024, 1024)

        return inputs.float(), labels.float()

    def _random_crop(self):
        """
        Generate a random crop position.

        Since patch_size = 1024 and crop_size = 512,
        x and y can range from 0 to 512.
        """

        x = random.randint(
            0,
            self.patch_size - self.crop_size
        )

        y = random.randint(
            0,
            self.patch_size - self.crop_size
        )

        return x, y

    def _get_train_transforms(self):
        """
        Augment training samples using random flips.

        ToTensorV2 converts:
            image -> torch tensor
            mask  -> torch tensor
        """

        return A.Compose([
            A.VerticalFlip(p=0.5),
            A.HorizontalFlip(p=0.5),
            ToTensorV2(transpose_mask=True)
        ])

    def _get_val_transforms(self):
        """
        Validation uses no random augmentation.
        Only convert NumPy arrays to PyTorch tensors.
        """

        return A.Compose([
            ToTensorV2(transpose_mask=True)
        ])



# Testing the train and test dataloaders
patches = glob.glob('/dmidata/projects/asip-cms/cgf/zarr_files2/*/*.zarr')

train_patches = patches[0:50]
test_patches = patches[0:3]

traindataset = CustomDataset(patch_paths=train_patches, is_train=True)
testdataset = CustomDataset(patch_paths=test_patches, is_train=False)
trainloader = torch.utils.data.DataLoader(traindataset, batch_size=8, num_workers=8, shuffle=True, pin_memory=True)
testloader = torch.utils.data.DataLoader(testdataset, batch_size=8, num_workers=8, shuffle=False, pin_memory=True)

train_iterator = iter(trainloader)
test_iterator = iter(testloader)

inputs, labels = next(train_iterator)
n = 4 #example 4 within the batch
fig, ax = plt.subplots(1, 5, figsize=(18, 5))
ax[0].imshow(inputs[n][0], vmin=np.nanpercentile(inputs[n][0], 1), vmax=np.nanpercentile(inputs[n][0], 99), cmap='gist_gray')
ax[1].imshow(inputs[n][1], vmin=np.nanpercentile(inputs[n][1], 1), vmax=np.nanpercentile(inputs[n][1], 99), cmap='gist_gray')
ax[2].imshow(inputs[n][2], vmin=np.nanpercentile(inputs[n][2], 1), vmax=np.nanpercentile(inputs[n][2], 99), cmap='gist_gray')
ax[3].imshow(inputs[n][3], vmin=np.nanpercentile(inputs[n][3], 1), vmax=np.nanpercentile(inputs[n][3], 99), cmap='gist_gray')
ax[4].imshow(labels[n][0])


# Custom Weighted Loss

class WeightedBCELoss(nn.Module):
    def __init__(self, class_weights=None):
        super(WeightedBCELoss, self).__init__()

        self.BCELoss = nn.BCEWithLogitsLoss(
            weight=class_weights,
            reduction='none'
        )

    def forward(
        self,
        input: torch.Tensor,
        target: torch.Tensor,
        weight_map: torch.Tensor = None
    ):
        # 1. Identify pixels with a valid target value
        valid_mask = torch.isfinite(target)

        # 2. Temporarily replace NaNs so BCE can be computed
        #    NaN pixels will be ignored later using valid_mask
        target_clean = torch.nan_to_num(target, nan=0.0)

        # 3. Compute the BCE loss for each pixel
        loss = self.BCELoss(input, target_clean)

        # 4. Apply the pixel-wise weight map, if provided
        if weight_map is not None:
            loss = loss * weight_map

        # 5. Keep only pixels with a valid target
        loss = loss[valid_mask]

        # 6. Compute the mean loss only over valid pixels
        return loss.mean()

class Trainer():
    def __init__(
            self, 
            model,
            criterion,
            optimizer,
            dataloaders,
            epochs,
            lr_scheduler=None,
            grad_scaler=None,
            resume_from_epoch=0,
            device='cuda',
            save_path=None,
            wandb=False
    ):
        self.model = model
        self.criterion = criterion
        self.optimizer = optimizer
        self.dataloaders = dataloaders
        self.epochs = epochs
        self.lr_scheduler = lr_scheduler
        self.grad_scaler = grad_scaler
        self.resume_from_epoch = resume_from_epoch
        self.device = device
        self.save_path = save_path
        self.wandb = wandb

        if self.resume_from_epoch != 0:
            self._load_checkpoint()

    def fit(self):
        print(f'[{datetime.datetime.now().isoformat(sep= " ", timespec="seconds")}] Training started')

        if self.save_path:
            if not os.path.exists(self.save_path):
                os.makedirs(self.save_path)

        for epoch in range(self.resume_from_epoch + 1, self.epochs + 1):
            for phase in self.dataloaders.keys():
                self.model.train() if phase == 'train' else self.model.eval()
                dataloader = self.dataloaders[phase]

                self._step(dataloader, phase, epoch)

            if self.lr_scheduler:
                self.lr_scheduler.step()

            if self.save_path:
                self._save_checkpoint(epoch)

    def _step(self, dataloader, phase, epoch):
        running_loss = 0

        for batch in tqdm(dataloader):
            inputs, labels = batch
            inputs = inputs.to(self.device)
            labels = labels.to(self.device)

            self.optimizer.zero_grad()
            with torch.set_grad_enabled(phase == 'train'):
                with torch.amp.autocast(self.device, enabled=self.grad_scaler is not None):
                    preds = self.model(inputs)
                    loss = self.criterion(preds, labels) # invlaid mask

                if phase == 'train':
                    if self.grad_scaler:
                        self.grad_scaler.scale(loss).backward()
                        self.grad_scaler.step(self.optimizer)
                        self.grad_scaler.update()
                    else:
                        loss.backward()
                        self.optimizer.step()
            running_loss += loss.item()*labels.size(0)

        if self.save_path:
            logged_loss = running_loss/len(dataloader.dataset)
            self._save_loss_to_json(phase=phase, epoch=epoch, loss=logged_loss)

            if self.wandb:
                wandb.log({f"{phase}_total_loss": logged_loss}, step=epoch)
            
        print(f'[{datetime.datetime.now().isoformat(sep= " ", timespec="seconds")}] Epoch: {epoch}   {phase}_loss: {running_loss/len(dataloader.dataset):.2f}')

    def _save_checkpoint(self, epoch):
        state_dict = {
            'model': self.model.state_dict(),
            'optimizer': self.optimizer.state_dict(),
            'epoch': epoch
        }
        if self.lr_scheduler:
            state_dict['lr_scheduler'] = self.lr_scheduler.state_dict()
        if self.grad_scaler:
            state_dict['grad_scaler'] = self.grad_scaler.state_dict()

        torch.save(state_dict, os.path.join(self.save_path, f'{epoch}.pt'))

    def _load_checkpoint(self):
        checkpoint = torch.load(os.path.join(self.save_path, f'{self.resume_from_epoch}.pt'))
        self.model.load_state_dict(checkpoint['model'])
        self.optimizer.load_state_dict(checkpoint['optimizer'])
        if self.lr_scheduler:
            self.lr_scheduler.load_state_dict(checkpoint['lr_scheduler'])
        if self.grad_scaler:
            self.grad_scaler.load_state_dict(checkpoint['grad_scaler'])

    def _save_loss_to_json(self, phase, epoch, loss, prefix=''):
        path = os.path.join(self.save_path, f'{prefix}{phase}_loss_log.json')
        if not os.path.exists(path):
            with open(path, 'w') as f:
                json.dump([], f, indent=2)

        with open(path, 'r') as f:
            loss_log = json.load(f)

        if len(loss_log) >= epoch:
            loss_log[epoch-1] = loss
        else:
            loss_log.append(loss)

        with open(path, 'w') as f:
            json.dump(loss_log, f, indent=2)




all_patches = glob.glob(
    '/dmidata/projects/asip-cms/cgf/zarr_files2/*/*.zarr'
)

train_patches = []
val_patches = []

for patch in all_patches:

    timestamp = os.path.basename(
        os.path.dirname(patch)
    )

    year = timestamp[:4]

    if year == '2020':
        val_patches.append(patch)
    else:
        train_patches.append(patch)

print("Train patches:", len(train_patches))
print("Validation patches:", len(val_patches))

traindataset = CustomDataset(patch_paths=train_patches, is_train=True)
testdataset = CustomDataset(patch_paths=test_patches, is_train=False)
trainloader = torch.utils.data.DataLoader(traindataset, batch_size=4, num_workers=4, shuffle=True, pin_memory=True)
testloader = torch.utils.data.DataLoader(testdataset, batch_size=1, num_workers=4, shuffle=False, pin_memory=True)

dataloaders = {
    'train': trainloader,
    'val': testloader
}

model = UNet(in_channels=5, out_channels=1, dims=[16, 32, 64, 128, 256, 512])
criterion = WeightedBCELoss()
optimizer = torch.optim.AdamW(params=model.parameters(), lr=3e-4, eps=1e-4)
lr_scheduler = torch.optim.lr_scheduler.MultiStepLR(optimizer, milestones=[30, 40], gamma=0.1)
grad_scaler = None

#device='cpu'
device = 'cuda:2'

trainer = Trainer(
    model=model.to(device),
    criterion=criterion,
    optimizer=optimizer,
    dataloaders=dataloaders,
    epochs=1,
    lr_scheduler=lr_scheduler,
    grad_scaler=grad_scaler,
    resume_from_epoch=0,
    device=device,
    save_path=None,
    wandb=False,
)

trainer.fit()