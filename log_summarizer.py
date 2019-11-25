from log_summarizer_functions import build_csv, count_unique_request, parse_log
from pipeline import Pipeline
import io


def log_summarizer(file):
    pipeline = Pipeline()

    @pipeline.task()
    def parse_logs(log):
        return parse_log(log)

    @pipeline.task(depends_on=parse_logs)
    def build_raw_csv(lines):
        return build_csv(lines, header=[
            'ip', 'time_local', 'request_type',
            'request_path', 'status', 'bytes_sent',
            'http_referrer', 'http_user_agent'
        ],
        file=io.StringIO())

    @pipeline.task(depends_on=build_raw_csv)
    def count_uniques(csv_file):
        return count_unique_request(csv_file)

    @pipeline.task(depends_on=count_uniques)
    def summarize_csv(lines):
        return build_csv(lines, header=['request_type', 'count'], file=io.StringIO())

    log = open(file)
    summarized_csv = pipeline.run(log)
    print(summarized_csv.readlines())


if __name__ == "__main__":
    log_summarizer('example_log.txt')
