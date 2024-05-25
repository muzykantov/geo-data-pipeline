import gzip
import io
import logging
import os
import tarfile

import luigi
import pandas as pd
import requests

logger = logging.getLogger("luigi-interface")

DATA_DIR = "data"
DATASET_NAME = "GSE68849"
DATASET_SERIES = "GSE68nnn"


class DownloadGeoDataset(luigi.Task):
    data_directory = luigi.Parameter(default=DATA_DIR)
    dataset_name = luigi.Parameter(default=DATASET_NAME)
    dataset_series = luigi.Parameter(default=DATASET_SERIES)

    def output(self):
        return luigi.LocalTarget(
            os.path.join(
                self.data_directory,
                f"{self.dataset_name}_{self.dataset_series}_RAW.tar",
            )
        )

    def run(self):
        os.makedirs(self.data_directory, exist_ok=True)
        url = self._construct_download_url(self.dataset_series, self.dataset_name)
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            with open(self.output().path, "wb") as file:
                file.write(response.content)
        else:
            raise Exception(
                f"Failed to download file with status code {response.status_code}"
            )

    def complete(self):
        if os.path.isfile(self.output().path):
            return os.stat(self.output().path).st_size > 0
        return False

    def _construct_download_url(self, dataset_series, dataset_name):
        return f"https://ftp.ncbi.nlm.nih.gov/geo/series/{dataset_series}/{dataset_name}/suppl/{dataset_name}_RAW.tar"


class ExtractAndProcessGeoDataset(luigi.Task):
    data_directory = luigi.Parameter(default=DATA_DIR)
    dataset_name = luigi.Parameter(default=DATASET_NAME)
    dataset_series = luigi.Parameter(default=DATASET_SERIES)

    def requires(self):
        return DownloadGeoDataset(
            data_directory=self.data_directory,
            dataset_name=self.dataset_name,
            dataset_series=self.dataset_series,
        )

    def output(self):
        extract_path = os.path.join(
            self.data_directory, self.dataset_name, self.dataset_series
        )
        return luigi.LocalTarget(extract_path)

    def run(self):
        os.makedirs(self.output().path, exist_ok=True)
        tar_path = self.input().path

        with tarfile.open(tar_path, "r") as tar:
            members = tar.getmembers()
            for member in members:
                file_name = os.path.basename(member.name)
                file_dir = os.path.join(self.output().path, file_name)
                os.makedirs(file_dir, exist_ok=True)
                member.name = os.path.basename(member.name)
                tar.extract(member, path=file_dir)

                extracted_file_path = os.path.join(file_dir, file_name)
                if extracted_file_path.endswith(".gz"):
                    self._decompress_and_split_gz(extracted_file_path, file_dir)

    def complete(self):
        extract_path = self.output().path
        if not os.path.isdir(extract_path):
            return False

        for root, dirs, _ in os.walk(extract_path):
            for dir_name in dirs:
                dir_path = os.path.join(root, dir_name)
                tsv_files = [f for f in os.listdir(dir_path) if f.endswith(".tsv")]
                if len(tsv_files) == 0:
                    return False

        return True

    def _decompress_and_split_gz(self, gz_path, output_dir):
        with gzip.open(gz_path, "rb") as file_in:
            content = file_in.read().decode("utf-8")
        os.remove(gz_path)
        self._process_content(content, output_dir)

    def _process_content(self, content, output_dir):
        data_frames = {}
        current_key = None
        string_io = io.StringIO()
        for line in content.splitlines():
            if line.startswith("["):
                if current_key:
                    string_io.seek(0)
                    header = None if current_key == "Heading" else "infer"
                    data_frames[current_key] = pd.read_csv(
                        string_io, sep="\t", header=header
                    )
                    output_path = os.path.join(output_dir, f"{current_key}.tsv")
                    data_frames[current_key].to_csv(output_path, sep="\t", index=False)
                string_io = io.StringIO()
                current_key = line.strip("[]\n")
                continue
            if current_key:
                string_io.write(line + "\n")
        if current_key:
            string_io.seek(0)
            data_frames[current_key] = pd.read_csv(string_io, sep="\t")
            output_path = os.path.join(output_dir, f"{current_key}.tsv")
            data_frames[current_key].to_csv(output_path, sep="\t", index=False)


class ProcessProbes(luigi.Task):
    data_directory = luigi.Parameter(default=DATA_DIR)
    dataset_name = luigi.Parameter(default=DATASET_NAME)
    dataset_series = luigi.Parameter(default=DATASET_SERIES)

    def requires(self):
        return ExtractAndProcessGeoDataset(
            data_directory=self.data_directory,
            dataset_name=self.dataset_name,
            dataset_series=self.dataset_series,
        )

    def output(self):
        outputs = []
        base_path = os.path.join(
            self.data_directory, self.dataset_name, self.dataset_series
        )
        for root, _, files in os.walk(base_path):
            if "Probes.tsv" in files:
                outputs.append(
                    luigi.LocalTarget(os.path.join(root, "ProbesTrimmed.tsv"))
                )
        return outputs

    def run(self):
        base_path = os.path.join(
            self.data_directory, self.dataset_name, self.dataset_series
        )
        for root, _, files in os.walk(base_path):
            if "Probes.tsv" in files:
                full_probes_path = os.path.join(root, "Probes.tsv")
                df = pd.read_csv(full_probes_path, sep="\t")
                columns_to_remove = [
                    "Definition",
                    "Ontology_Component",
                    "Ontology_Process",
                    "Ontology_Function",
                    "Synonyms",
                    "Obsolete_Probe_Id",
                    "Probe_Sequence",
                ]
                trimmed_df = df.drop(columns=columns_to_remove, errors="ignore")
                output_file_path = os.path.join(root, "ProbesTrimmed.tsv")
                trimmed_df.to_csv(output_file_path, sep="\t", index=False)

    def complete(self):
        base_path = os.path.join(
            self.data_directory, self.dataset_name, self.dataset_series
        )
        found_probes = False
        all_trimmed = True

        for root, _, files in os.walk(base_path):
            if "Probes.tsv" in files:
                found_probes = True
                trimmed_path = os.path.join(root, "ProbesTrimmed.tsv")
                if not (
                    os.path.exists(trimmed_path) and os.path.getsize(trimmed_path) > 0
                ):
                    all_trimmed = False
                    break

        return found_probes and all_trimmed


if __name__ == "__main__":
    luigi.run()
