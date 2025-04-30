# navp_votes

Fetch roll-call votes from Congress.gov and build a votes matrix.


## Setup

1. Clone this repo and `cd navp_votes/`
2. Create & activate your Python environment (pyenv, venv, conda, etc.)
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
4. Export your API key (get one at https://api.congress.gov/sign-up/) :
   ```bash
   export CONGRESS_API_KEY="YOUR_KEY"
   
## Usage
Fetch and store votes
```bash
python run_votes.py \
  --bills 118:hr:8034 118:hr:6090 118:hr:340 \
  --db votes.db