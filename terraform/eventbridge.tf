# Imported EventBridge Scheduler resources. This file is written to match the
# existing AWS scheduler and its dedicated execution role.

# Trust policy for EventBridge Scheduler.
data "aws_iam_policy_document" "scheduler_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [var.aws_account_id] # Match the existing AWS trust policy condition.
    }
  }
}

# Dedicated role used by EventBridge Scheduler to start the Step Functions run.
resource "aws_iam_role" "scheduler_exec" {
  name               = "Amazon_EventBridge_Scheduler_SFN_a5a3e3510b"
  path               = "/service-role/" # Match the existing AWS IAM role path to avoid replacement.
  assume_role_policy = data.aws_iam_policy_document.scheduler_assume_role.json
}

# Inline policy limited to starting this one state machine.
data "aws_iam_policy_document" "scheduler_start_execution" {
  statement {
    actions = ["states:StartExecution"]

    resources = [
      aws_sfn_state_machine.pipeline.arn,
    ]
  }
}

resource "aws_iam_role_policy" "scheduler_start_execution" {
  name   = "SchedulerStartTransfermarktPipeline"
  role   = aws_iam_role.scheduler_exec.id
  policy = data.aws_iam_policy_document.scheduler_start_execution.json
}

# EventBridge Scheduler weekly trigger.
resource "aws_scheduler_schedule" "weekly" {
  name                         = "sport_analysis_weekly"
  group_name                   = "default"
  schedule_expression          = "cron(0 12 ? * MON *)"
  schedule_expression_timezone = "America/Chicago"
  state                        = "ENABLED"
  action_after_completion      = "NONE"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_sfn_state_machine.pipeline.arn
    role_arn = aws_iam_role.scheduler_exec.arn
    input = jsonencode({
      input = {
        league_id = "GB1"
        seasons   = ["2025"]
      }
    })

    retry_policy {
      maximum_event_age_in_seconds = 3660
      maximum_retry_attempts       = 1
    }
  }
}

output "scheduler_weekly_name" {
  description = "Name of the weekly EventBridge Scheduler schedule."
  value       = aws_scheduler_schedule.weekly.name
}

output "scheduler_weekly_arn" {
  description = "ARN of the weekly EventBridge Scheduler schedule."
  value       = aws_scheduler_schedule.weekly.arn
}
