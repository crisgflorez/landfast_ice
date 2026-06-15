# Basic UNet implementation

import torch
import torch.nn as nn
from torchinfo import summary

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