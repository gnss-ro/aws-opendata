variable "name" {
  description = "aws version of the contributed code"
}

variable "ecr_url" {
  description = "url of the ecr repository"
}

variable "batch_role_arn" {
  description = "batch role arn for job definitions"
}

resource "aws_batch_job_definition" "job_definition" {
  name = "${var.name}"
  type = "container"
  container_properties = <<CONTAINER_PROPERTIES
  {
    "image": "${var.ecr_url}",
    "memory": 1900,
    "vcpus": 1,
    "job_role": "${var.batch_role_arn}",
    "privileged": true
  }
CONTAINER_PROPERTIES

  retry_strategy {
    attempts = 2
    evaluate_on_exit {
        action = "EXIT"
        on_exit_code = 137
      }

    evaluate_on_exit {
        action = "EXIT"
        on_exit_code = 1
      }

    evaluate_on_exit {
        action = "RETRY"
        on_status_reason = "Host EC2*"
      }
  }
}
