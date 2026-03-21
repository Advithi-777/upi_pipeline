$files = @(
  "src\__init__.py",
  "src\simulator\__init__.py",
  "src\simulator\upi_simulator.py",
  "src\transforms\__init__.py",
  "src\transforms\silver.py",
  "src\transforms\gold.py",
  "src\loaders\__init__.py",
  "src\loaders\snowflake_loader.py",
  "dags\upi_pipeline_dag.py",
  "tests\__init__.py",
  "tests\test_silver.py",
  "tests\test_gold.py",
  "configs\config.yaml",
  "configs\.env.example",
  ".gitignore",
  "requirements.txt",
  "README.md"
)
$files | ForEach-Object { New-Item -ItemType File -Force -Path $_ }