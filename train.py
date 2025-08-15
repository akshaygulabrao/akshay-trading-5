import torch
import torch.nn as nn
import numpy as np
import matplotlib.pyplot as plt

import pandas as pd
import numpy as np

FREQ       = '5min'
HIST_HRS   = 72
PRED_HRS   = 41
TOTAL_HRS  = HIST_HRS + PRED_HRS + 100     # add cushion
N_ROWS     = int(TOTAL_HRS * 60 / 5)       # 1 356 + 1 200 = 2 556 rows
D          = 3

start    = pd.Timestamp('2025-08-10 00:00:00')
dt_idx   = pd.date_range(start, periods=N_ROWS, freq=FREQ, name='timestamp')

np.random.seed(42)

def make_channel(base_freq, noise=0.25, phase=0.0):
    t = np.arange(len(dt_idx))
    return np.sin(2*np.pi*base_freq*t + phase) + noise*np.random.randn(len(t))

df = pd.DataFrame(index=dt_idx)
df['ch0'] = make_channel(1/288)
df['ch1'] = make_channel(1/144)
df['ch2'] = make_channel(1/96)

print(df.shape)   # (2556, 3)  -> ~1 200 usable windows

# ------------------------------------------------------------------
# 0. Hyper-parameters & device
# ------------------------------------------------------------------
HIDDEN  = 64
LR      = 1e-3
EPOCHS  = 2_000
DEVICE  = 'cuda' if torch.cuda.is_available() else 'cpu'

# ------------------------------------------------------------------
# 1. Re-define window constants (same as before)
# ------------------------------------------------------------------
window_len = int((HIST_HRS + PRED_HRS) * 60 / 5)   # 1134 steps
hist_len   = int(HIST_HRS * 60 / 5)                # 864
pred_len   = int(PRED_HRS * 60 / 5)                # 492

# ------------------------------------------------------------------
# 2. Build sliding-window dataset
# ------------------------------------------------------------------
class SineDataset(torch.utils.data.Dataset):
    def __init__(self, series, window_len, hist_len, pred_len):
        self.series     = torch.tensor(series, dtype=torch.float32)
        self.window_len = window_len
        self.hist_len   = hist_len
        self.pred_len   = pred_len

    def __len__(self):
        return len(self.series) - self.window_len + 1

    def __getitem__(self, idx):
        full = self.series[idx:idx + self.window_len]
        x = full[:self.hist_len].unsqueeze(-1)          # (T_hist, 1)
        y = full[self.hist_len:]                        # (T_pred,)
        return x, y

# Use only channel 0 again
dataset = SineDataset(df['ch0'].values, window_len, hist_len, pred_len)

# Reserve last 10 % of windows for validation
n_total   = len(dataset)
n_train   = int(0.9 * n_total)
n_val     = n_total - n_train
train_ds, val_ds = torch.utils.data.random_split(
    dataset, [n_train, n_val],
    generator=torch.Generator().manual_seed(42)
)

BATCH = 64
train_loader = torch.utils.data.DataLoader(train_ds, batch_size=BATCH,
                                           shuffle=True, drop_last=True)
val_loader   = torch.utils.data.DataLoader(val_ds,   batch_size=BATCH,
                                           shuffle=False)

print(f"Train windows: {len(train_ds)} | Val windows: {len(val_ds)}")

# ------------------------------------------------------------------
# 3. Model
# ------------------------------------------------------------------
class NextStepLSTM(nn.Module):
    def __init__(self, hidden):
        super().__init__()
        self.lstm = nn.LSTM(input_size=1, hidden_size=hidden, batch_first=True)
        self.fc   = nn.Linear(hidden, 1)

    def forward(self, x):
        out, _ = self.lstm(x)          # (B, T, H)
        return self.fc(out)            # (B, T, 1)

model = NextStepLSTM(HIDDEN).to(DEVICE)
criterion = nn.MSELoss()
optimizer = torch.optim.Adam(model.parameters(), lr=LR)

# ------------------------------------------------------------------
# 4. Training loop
# ------------------------------------------------------------------
for epoch in range(1, EPOCHS+1):
    # ---- train ----
    model.train()
    train_loss = 0.0
    for xb, yb in train_loader:
        xb, yb = xb.to(DEVICE), yb.to(DEVICE)
        out = model(xb)                     # (B, T_hist, 1)
        pred = out.squeeze(-1)[:, :-1]      # (B, T_hist - 1)  -> predict next step
        target = xb.squeeze(-1)[:, 1:]      # (B, T_hist - 1)  -> shifted input
        loss = criterion(pred, target)
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()
        train_loss += loss.item() * xb.size(0)
    train_loss /= len(train_ds)

    # ---- val ----
    model.eval()
    val_loss = 0.0
    with torch.no_grad():
        for xb, yb in val_loader:
            xb, yb = xb.to(DEVICE), yb.to(DEVICE)
            out = model(xb)
            pred = out.squeeze(-1)[:, :-1]
            target = xb.squeeze(-1)[:, 1:]
            val_loss += criterion(pred, target).item() * xb.size(0)
    val_loss /= len(val_ds)

    if epoch % 10 == 0 or epoch == 1:
        print(f"epoch {epoch:4d} | train {train_loss:.6f} | val {val_loss:.6f}")

# ------------------------------------------------------------------
# 5. Quick visual sanity check on one validation window
# ------------------------------------------------------------------
model.eval()
with torch.no_grad():
    x_sample, y_sample = val_ds[0]
    x_sample = x_sample.unsqueeze(0).to(DEVICE)          # (1, T_hist, 1)
    pred_sample = model(x_sample).squeeze().cpu().numpy()

plt.figure(figsize=(8,3))
plt.title("Validation sample – history reconstruction")
plt.plot(y_sample[:hist_len], label='target')
plt.plot(pred_sample, '--', label='prediction')
plt.legend(); plt.tight_layout(); plt.show()