from dir_sync_helper_funcs import parse_arguments
from dir_sync_flow import directory_sync
from timeloop import Timeloop
from datetime import timedelta

tl = Timeloop()


def main():
    args = parse_arguments()

    @tl.job(interval=timedelta(seconds=args.interval * 60))
    def sync_job():
        directory_sync(args.source_dir_path, args.replica_dir_path, args.log_file_path)

    tl.start(block=True)


if __name__ == "__main__":
    main()
