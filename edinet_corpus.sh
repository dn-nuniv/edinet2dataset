#!/bin/bash


doc_types=(
    "annual" 
    # "quarterly" 
    # "semiannual" 
    # "annual_ammended" 
    # "quarterly_ammended" 
    # "semiannual_ammended"
)
for year in {2025..2026}   # 12/1に2022まで終了。次は2023-2025を行う。
do 
    for doc_type in "${doc_types[@]}"
    do
    for month in {1..12}
    do
        start_date="${year}-$(printf "%02d" $month)-01"

        # 月が12の場合は翌年の1月にする
        if [ "$month" -eq 12 ]; then
        end_date="$((year + 1))-01-01"
        else
        end_date="${year}-$(printf "%02d" $((month + 1)))-01"
        fi

        echo "doc_type: $doc_type, start_date: $start_date, end_date: $end_date"
        uv run python scripts/prepare_edinet_corpus.py --doc_type "$doc_type" --start_date "$start_date" --end_date "$end_date"
    done
    done
done
