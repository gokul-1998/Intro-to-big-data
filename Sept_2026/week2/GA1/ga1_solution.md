Viewed ga1.md:1-33
Viewed ga1_solution.py
Viewed input.txt:1-9
Searched for "longest"
Created ga1_solution.py
Viewed ga1_solution.py:1-124

I have created the complete assignment solution script in [ga1_solution.py](file:///d:/gokul_repos/iitm/Intro-to-big-data/Sept_2026/week2/GA1/ga1_solution.py).

### How it fulfills all assignment requirements:
1. **Downloads the file from GCS**: Pulls `wk_1_input.txt` from bucket `ibd_bucket_gokul`.
2. **Finds longest line(s)**: Calculates character lengths (stripping newline characters). If multiple lines share the maximum length, all are reported along with their line numbers.
3. **Prints results to terminal**: Formats and prints a clean summary and the line contents to stdout.
4. **Saves output to GCS**: Saves the formatted report to a local `output.txt` and uploads it to `gs://ibd_bucket_gokul/output.txt`.

---

### Running on your GCP VM:

Pull the changes or run directly:
```bash
git pull
```

Run with the default settings (`ibd_bucket_gokul` & `wk_1_input.txt`):
```bash
python3 ga1_solution.py
```

Or with custom parameters if needed:
```bash
python3 ga1_solution.py --bucket ibd_bucket_gokul --input-blob wk_1_input.txt --output-blob output.txt
```

---

### For Submission:
- **Code file**: [ga1_solution.py](file:///d:/gokul_repos/iitm/Intro-to-big-data/Sept_2026/week2/GA1/ga1_solution.py)
- **Output file**: Download `output.txt` from `gs://ibd_bucket_gokul/output.txt` via GCP Console or CLI:
  ```bash
  gsutil cp gs://ibd_bucket_gokul/output.txt .
  ```