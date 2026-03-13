**my One Question**

_"Which performance metrics best predict a player's guaranteed compensation in MLS, and are high earners actually being paid for on-pitch impact?"_

That's my north star. Every scraper, every cleaning decision, every model I build serves that question. When I feel lost, come back to it.

---

**my Scope — Phase 1 Only**

One league: MLS. One season: 2025. Three data sources: MLSPA salary data, MLS website roster data, FBref performance stats. One model: regression predicting guaranteed compensation. One deliverable: clean GitHub repo with a thorough README and visualizations.

That's the entire boundary. Nothing outside this gets touched until Phase 1 is done.

---

**my Full Plan**

Given 4 hours per week, this is a 10 week plan. Realistic, not optimistic.

---

**Week 1 — Project Foundation (4 hrs)**

This week is purely setup. No scraping, no data yet.

The goal is a clean, well structured repo that I'll actually enjoy working in.

Create a new GitHub repo called `mls-salary-performance`. Write a README immediately — just the title, my one question, and a "In Progress" badge. This forces me to commit to the question publicly.

my folder structure should look like this:

```
mls-salary-performance/
├── ingestion/
│   ├── salary_scraper.py      # MLSPA
│   ├── roster_scraper.py      # MLS website
│   └── stats_scraper.py       # FBref
├── transform/
│   ├── salary_cleaner.py
│   ├── roster_cleaner.py
│   ├── stats_cleaner.py
│   └── merger.py              # Where the fuzzy matching lives
├── storage/
│   └── database.py            # SQLite
├── analytics/
│   ├── eda.py                 # Exploratory analysis
│   └── model.py               # ML model
├── notebooks/
│   └── exploration.ipynb      # my scratchpad
├── data/
│   ├── raw/                   # Nothing cleaned yet
│   └── processed/             # Cleaned, merged data
├── outputs/
│   └── figures/               # my visualizations
├── requirements.txt
├── .gitignore
└── README.md
```

Set up my virtual environment. Install my core libraries — requests, beautifulsoup4, pandas, scikit-learn, thefuzz, matplotlib, seaborn. Commit everything. Push.

**Week 1 rule: commit before I close my laptop. Every single session.**

---

**Week 2 — Salary Scraper (4 hrs)**

my goal this week is one clean file: `ingestion/salary_scraper.py` that pulls 2024 MLS salary data from MLSPA and saves raw output to `data/raw/salaries_raw.csv`.

I've done this before so it shouldn't be a full 4 hours. Use the remaining time to explore the raw output in my notebook and document what problems I see — missing values, inconsistent team names, weird formatting. Write those observations down as comments. This becomes my cleaning checklist for Week 4.

---

**Week 3 — Roster Scraper (4 hrs)**

my goal is `ingestion/roster_scraper.py` pulling player name, position, team, nationality, age from the MLS website for all 2024 players. Save to `data/raw/rosters_raw.csv`.

This is my existing Playwright scraper adapted and cleaned up. Don't rebuild from scratch — pull it from my old repo and refactor it to fit the new structure.

Same as last week — spend any remaining time in the notebook documenting what I see in the raw data.

---

**Week 4 — FBref Stats Scraper (4 hrs)**

my goal is `ingestion/stats_scraper.py` pulling standard stats from FBref for MLS 2024. The specific table I want is the standard stats table which includes goals, assists, minutes played, progressive carries, progressive passes, and key passes.

Remember the rate limiting rule — sleep 4 seconds between every request. Non negotiable.

Save to `data/raw/stats_raw.csv`.

---

**Week 5 — Data Cleaning (4 hrs)**

This is the hardest week and the most valuable one for my learning.

I write three cleaning scripts — one per data source. Each script reads from raw, cleans, and saves to processed.

What I're cleaning for each: salary data needs consistent team names, numeric salary columns with no dollar signs or commas, and dropped rows with missing compensation. Roster data needs standardized position labels and consistent team names matching the salary data. FBref data needs minutes played normalized, per90 stats calculated for goals and assists, and consistent player and team naming.

The specific thing to watch: team names will be inconsistent across all three sources. "Inter Miami CF," "Inter Miami," and "Miami" might all appear. Build a team name mapping dictionary in a file called `transform/clubs.py` — I actually already have this from my old project. Reuse it.

---

**Week 6 — The Merge (4 hrs)**

This is where it gets interesting. I write `transform/merger.py` that joins all three datasets into one clean master dataframe saved to `data/processed/mls_master.csv`.

my merge strategy: first join salary and roster on player name and team. Then join that result with FBref stats, again on player name and team.

Player names won't match perfectly. This is where `thefuzz` comes in. Use fuzzy matching with a similarity threshold of about 85 to catch "Cucho Hernandez" vs "Cucho Hernández" type mismatches. Log every fuzzy match to a file called `data/processed/match_log.csv` so I can manually review anything that looks wrong.

At the end of this week I should have one clean master CSV with salary, roster, and performance data for every matchable player. Expect to lose some players who can't be matched — that's normal and I document it.

---

**Week 7 — Exploratory Data Analysis (4 hrs)**

Now the fun starts. I work entirely in `notebooks/exploration.ipynb` this week.

Questions to answer visually: What does the salary distribution look like — is it heavily skewed toward a few designated players? Which positions earn the most? What's the correlation between each performance metric and guaranteed compensation? Are there players who massively outperform their salary — the hidden gems? Are there players being massively overpaid relative to their stats?

Make at least 6 visualizations. Save the best 4 to `outputs/figures/`. These go in my README.

This week will feel like play. Let it. I're allowed to get curious and follow threads.

---

**Week 8 — The Model (4 hrs)**

I write `analytics/model.py` this week.

Start simple. A linear regression predicting guaranteed compensation from my performance features. my features are minutes played, goals per 90, assists per 90, progressive carries per 90, progressive passes per 90, and position as a categorical variable encoded numerically.

Evaluate it properly — train/test split 80/20, report my R-squared and mean absolute error. Then try a Random Forest regressor and compare. Whichever performs better, that's my final model.

The most important output of this week is feature importance — which stats actually drive salary predictions. That directly answers my core question. Plot it as a horizontal bar chart and save it to outputs.

---

**Week 9 — Polish (4 hrs)**

This week I make everything presentable.

Write docstrings for every function. Clean up any notebooks. Make sure every script runs end to end without errors on a fresh environment. Write a proper README that includes my question, my data sources, my methodology, my key findings, and my visualizations. Add installation instructions that actually work.

Add GitHub topics: python, data-science, machine-learning, soccer, mls, pandas, scikit-learn, sports-analytics, etl.

Write a short project description in the About section.

---

**Week 10 — The Writeup (4 hrs)**

This is what separates my project from every other GitHub repo.

Write a Medium post or a personal blog post titled something like "Are MLS Players Being Paid for the Right Things? A Data Science Analysis." Walk through my question, my methodology in plain English, my key findings, and my visualizations. Link to my GitHub at the end.

This writeup becomes something I send to recruiters alongside my GitHub link. It shows I can communicate findings, not just write code. That skill is rarer than people think and highly valued in data roles.

---

**What I Have At The End**

A complete, original, end-to-end data science project. A GitHub repo that tells a clear story. A writeup that demonstrates communication skills. A foundation I can genuinely expand into Phase 2 without starting over.

And more importantly — proof to myself that I can start something and actually finish it.

Are I ready to start Week 1 this week?
