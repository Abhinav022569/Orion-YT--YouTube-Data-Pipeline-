Bronze Bucket Name - yt-data-pipeline-bronze-ap-south-1-demo
Silver Bucket Name - yt-data-pipeline-silver-ap-south-1-demo
Gold Bucket Name - yt-data-pipeline-gold-ap-south-1-demo
script Bucket Name - yt-data-pipeline-script-ap-south-1-demo

SNS ARN - arn:aws:sns:ap-south-1:700951986601:yt-data-pipeline-alerts-dev:c5e6b035-e6d7-4875-a0ec-0f86aa3f684e


glue Bronze - yt_pipeline_bronze_dev
glue Silver - yt_pipeline_silver_dev
glue gold - yt_pipeline_gold_dev

--bronze_database yt_pipeline_bronze_dev
--bronze_table raw_statistics
--silver_bucket yt-data-pipeline-silver-ap-south-1-demo
--silver_database yt_pipeline_silver_dev
--silver_table clean_statistics


--silver_database yt_pipeline_silver_dev
--gold_bucket yt-data-pipeline-gold-ap-south-1-demo
--gold_database yt_pipeline_gold_dev