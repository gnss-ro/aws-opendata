# Security group used by AWS Batch instances
resource "aws_security_group" "batch" {
  name_prefix = "${var.tags.Name}-batch"
  vpc_id = var.vpc_id

  # inbound
  ingress {
    from_port = 0
    to_port = 0
    protocol = "-1"
    cidr_blocks = [var.subnet_cidr]
    description = "Ingress from private subnet"
  }

  # inbound
  ingress {
    from_port = 0
    to_port = 0
    protocol = "-1"
    cidr_blocks = [var.subnet_cidrProd]
    description = "Ingress from private subnet in prod"
  }

  # outbound access
  egress {
      from_port = 0
      to_port = 0
      protocol = "-1"
      cidr_blocks = ["0.0.0.0/0"]
  }
}
