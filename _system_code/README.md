# Shared course code

`paths.py` is the only implementation of `resolve_jobs_data_dir()`. It preserves
the `JOBS_DATA_DIR` override, Windows home/office detection, and EC2 dataset
discovery. The office machine uses `D:/Boston/AD688/Data/Jobs_2026_US`;
the home machine uses `E:/Data/Jobs_2026`; and `MET-PADALKAR-L` uses
`C:/Users/npadalka/Documents/Repositories/Jobs_2026`.
Notebook setup locates this package from the current working
directory or its parents before importing:

```python
from _system_code.paths import resolve_jobs_data_dir

data_dir = resolve_jobs_data_dir()
```

Keep this folder at the repository root. For a standalone QMD or notebook,
copy the entire `_system_code` folder beside the document or into a parent
folder. Start Jupyter or Quarto from that folder or a descendant. A missing
package produces an explicit setup error; there is no embedded fallback copy.

`research_survey.py` and `research_survey_workbook.py` retain the survey helpers
previously stored in `delivarables-solutions/_system_code`. Their output paths
remain relative to the notebook's working directory.

Maintain the notebook setup in `support_scripts/sync_jobs_loaders.py` and run it from
the repository root after changing the standard loader. Keep sample selection,
Spark reads, and exercise-specific preparation visible in the course documents.
