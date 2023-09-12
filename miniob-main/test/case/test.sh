python3 miniob_test.py \
        --test-case-dir=./test  \
        --test-case-scores=case-scores.json \
        --test-result-dir=result \
        --test-result-tmp-dir=./result_tmp \
        --use-unix-socket \
        --db-base-dir=/home/ubuntu/miniob-main \
        --code-type=None \
        --log=stdout \
        --test-cases=$1
