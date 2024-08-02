import argparse
import time

from phenolrs.pyg import PygLoader


def load_abide(host: str, password: str) -> None:
    data = PygLoader.load_into_pyg_heterodata(
        "abide",
        [{"name": "Subjects", "fields": ["label", "brain_fmri_features"]}],
        [{"name": "medical_affinity_graph"}],
        [host],
        username="root",
        password=password,
        parallelism=10,
        batch_size=400000,
    )
    assert data["Subjects"]["brain_fmri_features"].shape == (871, 2000)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("password", type=str)
    parser.add_argument("--host", type=str, default="http://localhost:8529")
    parser.add_argument("--trials", type=int, default=4)
    args = parser.parse_args()

    for _ in range(args.trials):
        start = time.perf_counter()
        load_abide(args.host, args.password)
        end = time.perf_counter()
        print(f"Total execution time: {end - start} seconds")


if __name__ == "__main__":
    main()
