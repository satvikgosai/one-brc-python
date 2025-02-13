from one_brc import main

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Process 1 billion rows from file")
    parser.add_argument("file", type=str, help="The path to the file to be processed")
    args = parser.parse_args()

    main(args.file)
