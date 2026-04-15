# Database cache time-to-live in hours.
# The local indicator DB is rebuilt from the UIS API when older than this.
DB_TTL_HOURS = 24

# Default number of results returned by search_indicators.
MAX_RESULTS = 20

# Hard cap on results a caller can request from search_indicators.
MAX_RESULTS_CAP = 50

# Maximum indicator codes accepted by get_indicator_summary.
MAX_SUMMARY_CODES = 10