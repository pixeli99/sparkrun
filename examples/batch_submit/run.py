

tasks = [
    "common_crawl-adult_content",
    "common_crawl-art_and_design",
    # "common_crawl-crime_and_law",
    # "common_crawl-education_and_jobs",
    # "common_crawl-electronics_and_hardware",
    # "common_crawl-entertainment",
    # "common_crawl-fashion_and_beauty",
    # "common_crawl-finance_and_business",
    # "common_crawl-food_and_dining",
    # "common_crawl-games",
    # "common_crawl-health",
    # "common_crawl-history_and_geography",
    # "common_crawl-home_and_hobbies",
    # "common_crawl-industrial",
    # "common_crawl-literature",
    # "common_crawl-politics",
    # "common_crawl-religion",
    # "common_crawl-science_math_and_technology",
    # "common_crawl-social_life",
    # "common_crawl-software",
    # "common_crawl-software_development",
    # "common_crawl-sports_and_fitness",
    # "common_crawl-transportation",
    # "common_crawl-travel_and_tourism",
    # "dolma1_7-wiki-en",
    # "finemath-3plus",
    # "olmocr_science_pdfs-adult_content",
    # "olmocr_science_pdfs-art_and_design",
    # "olmocr_science_pdfs-education_and_jobs",
    # "olmocr_science_pdfs-entertainment",
    # "olmocr_science_pdfs-games",
    # "olmocr_science_pdfs-sports_and_fitness",
    # "stack_edu-C",
    # "stack_edu-Cpp",
    # "stack_edu-CSharp",
    # "stack_edu-Go",
    # "stack_edu-Java",
    # "stack_edu-JavaScript",
    # "stack_edu-Markdown",
    # "stack_edu-PHP",
    # "stack_edu-Python",
    # "stack_edu-Ruby",
    # "stack_edu-Rust",
    # "stack_edu-Shell",
    # "stack_edu-SQL",
    # "stack_edu-Swift",
    # "stack_edu-TypeScript",
]

def task2adapter(task):
    if task.startswith("common_crawl"):
        return "dolma3_mix_cc"
    elif task.startswith("dolma1_7"):
        return "dolma3_mix_wiki"
    elif task.startswith("finemath"):
        return "dolma3_mix_math"
    elif task.startswith("olmocr_science_pdfs"):
        return "dolma3_mix_ocr"
    elif task.startswith("stack_edu"):
        return "dolma3_mix_code"
    else:
        raise ValueError(f"Unknown task: {task}")

cmd_template = """
FILE_TYPE="{FILE_TYPE}"
INPUT_PATH="{INPUT_PATH}"
OUTPUT_PATH="{OUTPUT_PATH}"
NUM_PARTITIONS="{NUM_PARTITIONS}"
ADAPTER="{ADAPTER}"
sbatch --job-name=standardize-{TASK} --reservation=megatron --nodes=16 --export=ALL,INPUT_PATH="$INPUT_PATH",OUTPUT_PATH="$OUTPUT_PATH",FILE_TYPE="$FILE_TYPE",NUM_PARTITIONS="$NUM_PARTITIONS",ADAPTER="$ADAPTER" \
    submit_plus.sh examples/fineweb_standardize/run_fineweb_standardize.sh
"""


for task in tasks:
    input_path = f"/work/projects/polyullm/congkai/pretrain_data/dolma3_mix-6T-1025/data/{task}*/*.jsonl"
    output_path = f"/work/projects/polyullm/reallm.xyz/spark-runner/demo/dolma3_mix-6T-1025/{task}"
    file_type = "json"
    num_partitions = "-1"
    adapter = task2adapter(task)
    
    cmd = cmd_template.format(
        INPUT_PATH=input_path,
        OUTPUT_PATH=output_path,
        FILE_TYPE=file_type,
        NUM_PARTITIONS=num_partitions,
        ADAPTER=adapter,
        TASK=task,
    )

    print(cmd)


