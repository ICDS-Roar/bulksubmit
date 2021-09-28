import argparse
import json
import multiprocessing
import os
import shutil
import subprocess
from typing import List


class PBSBuilder:
    def __init__(self, meta_data: dict, pbs_directives: dict, prepend_commands: dict, 
                 simulation_parameters: dict, append_commands: dict) -> None:
        self.meta_data = meta_data
        self.pbs_directives = pbs_directives
        self.prepend_commands = prepend_commands
        self.simulation_parameters = simulation_parameters
        self.append_commands = append_commands
        self.batch_dirs = None

    def build(self) -> str:
        # Format batch paramters
        batch_command_list = list()
        for sim in self.simulation_parameters:
            for batch in sim["batch_parameters"]:
                batch_command_list.append({batch["name"]: sim["root_command"].format(*batch["parameters"])})

        # Create output path
        try:
            os.chdir(self.meta_data["output_directory"])

        except OSError:
            os.makedirs(self.meta_data["output_directory"], exist_ok=True)
            os.chdir(self.meta_data["output_directory"])

        out = os.path.join(os.getcwd(), self.meta_data["simulation_name"])

        try:
            os.mkdir(out); os.chdir(out)

        except FileExistsError:
            shutil.rmtree(out); os.mkdir(out); os.chdir(out)

        # Create directory for each batch
        for batch in batch_command_list:
            for name in batch.keys():
                os.mkdir(name)

        # Grab the file paths to all of the batch dirs
        batch_dirs = self._getdirs(os.getcwd())
        for dir in batch_dirs:
            self._write_pbs(dir, batch_command_list)

        self.batch_dirs = batch_dirs

    def submit(self) -> None:
        process_list = list()
        for dir in self.batch_dirs:
            try:
                print(type(dir))
                process = multiprocessing.Process(target=self._qsub_launcher,
                                                  args=(dir,))
                process_list.append(process)
                process.start()

            except multiprocessing.ProcessError:
                print("Something went wrong trying to submit the batch jobs!")

        # Block until all jobs have been successfully submitted
        for process in process_list:
            process.join()

    def _getdirs(self, *paths) -> List:
        root_list = list()
        for path in paths:
            for root, directories, files in os.walk(path):
                for directory in directories:
                    root_list.append(os.path.join(root, directory))

        return root_list

    def _qsub_launcher(self, pbs_script_path: str) -> None:
        os.chdir(pbs_script_path)
        subprocess.run(["qsub", os.path.join(pbs_script_path, "submit.pbs")])

    def _write_pbs(self, outdir: str, batch_commands: List[dict]) -> None:
        with open(os.path.join(outdir, "submit.pbs"), "wt") as fout:
            # Get name of batch
            batch_name = outdir.split("/"); batch_name = batch_name[-1]

            # Write PBS directives
            for directive in self.pbs_directives:
                fout.write(directive + "\n")

            # Write commands that come before the batch commands
            for pre in self.prepend_commands:
                fout.write(pre + "\n")

            # Write batch commands
            for batch in batch_commands:
                if batch_name in batch:
                    fout.write(batch[batch_name] + "\n")

            # Write commands that come after the batch command
            for post in self.append_commands:
                fout.write(post + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("simulation_file", default=None)
    args = parser.parse_args()

    if args.simulation_file is None:
        print("No simulation file specified. Exiting.")
        exit(1)

    try:
        fin = open(args.simulation_file, "rt"); sim = fin.read(); fin.close()
        sim = json.loads(sim)

    except:
        raise IOError("Failed to parse JSON file. Please make sure there are no errors in your file.")

    # Initialize PBS script builder
    pbs = PBSBuilder(sim["meta_data"], sim["pbs_directives"], sim["prepend_commands"], 
                     sim["simulation_parameters"], sim["append_commands"])
    pbs.build()
    pbs.submit()
