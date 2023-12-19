# Workflow
  1. Lambda will
      a. scrape web to find all files listed on the site.
      b. scrape the liveupdate bucket for what we've processed
      c. get a difference of these two scrape datasets.
      d. submit batch job for each tar file to process

# Notes:
hard coded for metop only for now
