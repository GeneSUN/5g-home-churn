# 🕒 Time-Series Churn Classification

---

## 📌 Overview
This project explores how **time-series modeling** improves churn prediction by capturing early behavioral signals, avoiding leakage, and separating true causal effects from noise.

<img width="1236" height="773" alt="Screenshot 2025-11-02 at 5 44 37 PM" src="https://github.com/user-attachments/assets/fb0e24c2-83ee-40d2-ac11-997c390cea14" />

- https://medium.com/@injure21/time-series-classification-a-practical-field-guide-with-a-telco-churn-walkthrough-271fa59b9bd0
- https://medium.com/@injure21/time-series-classification-churn-c33f85a038fd
- https://colab.research.google.com/drive/1CGFJHqtr3R6KMDE4qNyd7sHLn0A4eg61
---

## 🚫 1. Avoid Temporal Leakage
- Add a **time gap** between observation and prediction windows.  
  e.g., use months 1–3 data → skip 4 → predict churn in 5–6.  
- Prevents the model from “cheating” on near-churn signals and supports proactive retention.  
- Focus on *actionability* over raw accuracy.

<img width="720" height="117" alt="image" src="https://github.com/user-attachments/assets/97af0bcb-fcd8-4030-a7e1-82faebda0329" />


---

## 🔄 2. Fuse Static & Temporal Features
Two integration approaches:
1. **Static-as-channel** – repeat static features across all timesteps.  
   ✅ works with off-the-shelf classifiers (InceptionTime, ResNet).  
   ❌ redundant representation.  
2. **Dual-input fusion** – LSTM/Conv branch for time-series + MLP for static features.  
   ✅ cleaner architecture, interpretable embeddings.

<img width="1100" height="127" alt="image" src="https://github.com/user-attachments/assets/18e786cc-2496-45b4-84c6-d9144e665c46" />


---

## ⚖️ 3. Mixed Causality & Dilution


<img width="460" height="257" alt="Screenshot 2025-11-02 at 7 28 25 PM" src="https://github.com/user-attachments/assets/c4185658-a1a0-47ce-a010-5aec43e36595" />


<img width="1355" height="489" alt="Screenshot 2025-11-02 at 7 27 27 PM" src="https://github.com/user-attachments/assets/a472e77f-7942-4361-b16e-6eb229ece5e7" />



---

## ⚙️ 4. Practical Notes
- **Actionability vs Accuracy:** early predictions are noisier but more useful — pick a lead time that maximizes business value.
- **Imbalance / Drift:** use weighted loss or threshold tuning.  
- **Label Noise:** define churn clearly (e.g., 60-day inactivity).  


---
