# GEO Data Processing Pipeline

This project is a data processing pipeline for downloading, extracting, and processing GEO datasets using Luigi. The pipeline consists of three main tasks: downloading the dataset, extracting and processing it, and processing probe data.

## Table of Contents
- [Installation](#installation)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Tasks](#tasks)
  - [DownloadGeoDataset](#downloadgeodataset)
  - [ExtractAndProcessGeoDataset](#extractandprocessgeodataset)
  - [ProcessProbes](#processprobes)
- [Configuration](#configuration)
- [License](#license)

## Installation

1. **Clone the repository:**
    ```sh
    git clone https://github.com/muzykantov/geo-data-pipeline.git
    cd geo-data-pipeline
    ```

2. **Create and activate a virtual environment:**
    ```sh
    python -m venv venv
    source venv/bin/activate  # On Windows use `venv\Scripts\activate`
    ```

3. **Install the required dependencies:**
    ```sh
    pip install -r requirements.txt
    ```

## Usage

Run the Luigi pipeline with the following command:

```sh
python geo_data_pipeline.py ProcessProbes --local-schedule
```

This command will execute all tasks in the pipeline, starting with downloading the dataset, extracting and processing it, and finally processing the probe data.

## Project Structure

```
geo-data-pipeline/
│
├── data/                        # Directory for storing downloaded and processed data
├── geo_data_pipeline.py         # Main script for Luigi tasks
├── requirements.txt             # Project dependencies
├── README.md                    # Project documentation
└── .gitignore                   # Git ignore file
```

## Tasks

### DownloadGeoDataset

This task downloads the specified GEO dataset from the NCBI FTP server.

- **Parameters:**
  - `data_directory`: Directory to store the downloaded dataset.
  - `dataset_name`: Name of the GEO dataset (e.g., `GSE68849`).
  - `dataset_series`: Series identifier (e.g., `GSE68nnn`).

### ExtractAndProcessGeoDataset

This task extracts the downloaded tar file and processes the content, decompressing any `.gz` files and splitting the content into TSV files.

- **Parameters:**
  - `data_directory`: Directory where the dataset is stored.
  - `dataset_name`: Name of the GEO dataset.
  - `dataset_series`: Series identifier.

### ProcessProbes

This task processes the `Probes.tsv` file by removing unnecessary columns and saving the trimmed data to `ProbesTrimmed.tsv`.

- **Parameters:**
  - `data_directory`: Directory where the dataset is stored.
  - `dataset_name`: Name of the GEO dataset.
  - `dataset_series`: Series identifier.

## Configuration

You can configure the default values for `data_directory`, `dataset_name`, and `dataset_series` in the `geo_data_pipeline.py` script or by passing them as parameters when running Luigi.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
