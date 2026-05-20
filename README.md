# big_data_lab

## Folder analysis

The repository is organized around Hadoop MapReduce lab programs.

- `/!bda lab/IdeaProjects/`
  - `WordCount/` – Java MapReduce implementation for word count (`src/main/java/org/wordcount`).
  - `weather/` – Java MapReduce implementation for weather aggregation (`src/main/java/org/weather`).
  - `student_grade/` – Java MapReduce implementation for student grading (`src/main/java/org/student_grade`).
  - MatrixMul – Java MapReduce implementation for matrix multiplication (`src/main/java/org/matrixmulti`), located in `MartixMul/`.
- `/!bda lab/input/` – sample input datasets used by the MapReduce jobs.
- `/!bda lab/output/` – sample/generated output files from executed jobs.
- `/!bda lab/jar files/` – built runnable JAR artifacts for the four jobs.
- `/!bda lab/java_code_to_write/` – standalone Java source files mirroring the core lab tasks.
- `/!bda lab/*.pdf` and `*.docx` – lab instructions/setup/reference documents.

### Build/Test

Each Maven project under `/!bda lab/IdeaProjects/*` has its own `pom.xml` and can be tested individually, for example:

```bash
mvn -f "/home/runner/work/big_data/big_data/!bda lab/IdeaProjects/WordCount/pom.xml" test
```
