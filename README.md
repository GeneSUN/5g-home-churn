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

```text
Dilution Effect of Mixed Causality
├── Problem: Multiple churn causes
│   ├── 20% due to network issues
│   ├── 80% due to non-network reasons
│   └── Single model → diluted signal
├── Solution: Two-Stage Pipeline
│   ├── Stage 1: Service-risk detector (CUSUM / LSTM-AE)
│   ├── Stage 2: Churn classifier using Stage-1 signals
│   └── Outcome: interpretable churn alerts (e.g., SNR↓ 25%)
└── Local Signal Principle (Heterogeneity-Aware Modeling)
    ├── Global models blur heterogeneous causes
    ├── Segmentation reveals coherent subpatterns
    └── Broader use: churn, credit, forecasting, medical risk
```

<img width="1355" height="489" alt="Screenshot 2025-11-02 at 7 27 27 PM" src="https://github.com/user-attachments/assets/a472e77f-7942-4361-b16e-6eb229ece5e7" />



---


## 🔃 4. Intervention Feedback Loops (Prediction Changes the Future)

Once a churn model goes into production, the prediction itself **triggers business actions**
(discounts, outreach, new plans).  
If those actions work, many predicted churners **don’t churn anymore**.

From the raw data perspective, it looks like the model was **wrong** —  
but in reality, the model was **right**, and the intervention changed the outcome.

This is known as:

- **Intervention Feedback Loop**
- **Post-prediction label distortion**
- **“False positives that were actually saved customers”**

Without guarding against this effect, model metrics become misleading:

| What happened in real life | How the raw data looks |
|----------------------------|------------------------|
| Model predicted churn → offer sent → customer stayed | Model appears incorrect |
| Model predicted churn → no intervention → customer churned | Model appears correct |
| Model predicted churn → customer would have stayed anyway | Real false positive |

So offline metrics like **precision, recall, F1-score** no longer tell the truth.

---

### 4.1 A/B Holdout (Gold Standard)

The simplest, most reliable fix is to **intentionally do nothing** for a small random sample.

**How it works**
- Model predicts a set of high-risk customers
- **Treated Group** receives discounts/retention actions
- **Control Group** receives **no** intervention

After a few billing cycles, compare churn rates:

| Group | Churn Rate |
|-------|------------|
| Treated | 6% |
| Control | 13% |

Even if half the treated group didn’t churn, it doesn’t mean they were false positives —  
the **control** shows they would have churned without intervention.

🎯 **This measures business impact, not classifier accuracy.**
🎯 **This is how telecom, banks, streaming, and insurance evaluate churn models.**

---

### 4.2 Shadow Evaluation (Operational Safety Check)

Sometimes you can’t run a full A/B test (too sensitive or too costly).  
A lighter alternative is **Shadow Evaluation**.

**How it works**
- Model continues to run normally
- But a small random sample of predicted churners are deliberately **not targeted**
- Their outcomes are tracked silently

This gives:
- Unbiased estimate of churn **without intervention**
- Early warning if the model’s true performance degrades
- Protection against silent failures in upstream data or feature pipelines

Shadow evaluation is especially useful:
- after model updates
- after feature refactors
- during early deployment
- when seasonality or promotions may distort data

---

### 4.3 Measure Business Metrics, not Precision

For churn, the real KPI is **revenue saved**, not a confusion matrix.

Raw ML metrics become unreliable because intervention changes labels.  
Instead, track:

- **Net churn reduction**
- **Revenue retained** from saved customers
- **Cost per saved customer**
- **Lift vs. baseline (control group)**
- **ROI of retention actions**

Example:

| Metric | Without model | With model + outreach |
|--------|---------------|----------------------|
| Churn rate | 14% | 7% |
| Monthly revenue loss | \$2.4M | \$900k |
| Promo cost | – | \$200k |
| **Net savings** | – | **\$1.3M** |

Even if the model’s precision looks mediocre, the **business impact** is excellent.

---

### Summary

| Problem | Consequence | Practical Fix |
|---------|-------------|---------------|
| Model triggers interventions | Labels get distorted | A/B Holdout (ground truth) |
| Cannot run full A/B | Need lightweight monitoring | Shadow Evaluation |
| Traditional ML metrics break | Precision/recall misleading | Measure churn reduction & ROI |

✅ These keep churn modeling **honest**,  
✅ separate **saved customers** from **false positives**,  
✅ and ensure the model is judged on **business value**, not just accuracy.

---

## ⚙️ 5. Practical Notes
- **Actionability vs Accuracy:** early predictions are noisier but more useful — pick a lead time that maximizes business value.
- **Imbalance / Drift:** use weighted loss or threshold tuning.  
- **Label Noise:** define churn clearly (e.g., 60-day inactivity).  

