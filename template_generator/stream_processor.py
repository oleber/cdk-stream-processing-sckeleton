import os

from aws_cdk import (
    aws_lambda,
    aws_lambda_event_sources,
    aws_events,
    aws_events_targets,
    aws_s3,
    aws_sqs,
    aws_sns,
    aws_sns_subscriptions,
    core,
)
from aws_cdk.core import Duration, Tag


class StreamProcessorStack(core.Stack):

    def __init__(self, scope: core.Construct, id: str, prefix: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        def create_s3(name, block_public_access=aws_s3.BlockPublicAccess.BLOCK_ACLS):
            s3 = aws_s3.Bucket(
                self,
                f"mrebelo-{name}",
                bucket_name=f"mrebelo-{name}",
                block_public_access=block_public_access
            )
            s3.add_lifecycle_rule(
                id="GlacierRule",
                prefix="to_transition",
                expiration=Duration.days(365),
                transitions=[
                    aws_s3.Transition(
                        storage_class=aws_s3.StorageClass.INTELLIGENT_TIERING,
                        transition_after=Duration.days(30)
                    ),
                    aws_s3.Transition(
                        storage_class=aws_s3.StorageClass.GLACIER,
                        transition_after=Duration.days(90)
                    )
                ]
            )
            Tag.add(s3, "name", f"mrebelo-{name}")

            return s3

        def create_fifo_sqs(name):
            sqs_dl = aws_sqs.Queue(
                self, f"sqs_{name}_dq",
                queue_name=f"mrebelo-{name}-dl.fifo",
                visibility_timeout=Duration.minutes(10),
                fifo=True
            )

            sqs = aws_sqs.Queue(
                self, f"sqs_{name}",
                queue_name=f"mrebelo-{name}.fifo",
                visibility_timeout=Duration.minutes(1),
                fifo=True,
                dead_letter_queue=aws_sqs.DeadLetterQueue(
                    max_receive_count=3,
                    queue=sqs_dl
                ),
                content_based_deduplication=True
            )

            return sqs

        def create_sqs(name):
            sqs_dl = aws_sqs.Queue(
                self, f"sqs_{name}_dq",
                queue_name=f"mrebelo-{name}-dl",
                visibility_timeout=Duration.minutes(10)
            )

            sqs = aws_sqs.Queue(
                self, f"sqs_{name}",
                queue_name=f"mrebelo-{name}",
                visibility_timeout=Duration.minutes(1),
                dead_letter_queue=aws_sqs.DeadLetterQueue(
                    max_receive_count=3,
                    queue=sqs_dl
                )
            )

            return sqs

        def create_lambda(
                name, code,
                s3_source=None,
                s3_destination=None,
                sqs_invoke=None,
                sqs_notify=None,
                environment={}
        ):
            lambdaFn = aws_lambda.Function(
                self, f"mrebelo-{name}",
                function_name=f"mrebelo-{name}",
                runtime=aws_lambda.Runtime.PYTHON_3_8,
                handler="lambda-handler.main",
                code=code,
                timeout=Duration.seconds(5),
                environment=environment.copy()
            )

            if s3_source is not None:
                lambdaFn.add_environment("S3_SOURCE_ARN", s3_source.bucket_arn)
                s3_source.grant_read(lambdaFn)

            if s3_destination is not None:
                lambdaFn.add_environment("S3_DESTINATION_ARN", s3_destination.bucket_arn)
                s3_destination.grant_write(lambdaFn)

            if sqs_notify is not None:
                lambdaFn.add_environment("SQS_NOTIFY_ARN", sqs_notify.queue_arn)
                lambdaFn.add_environment("SQS_NOTIFY_URL", sqs_notify.queue_url)
                sqs_notify.grant_send_messages(lambdaFn)

            if sqs_invoke is not None:
                lambdaFn.add_environment("SQS_INVOKING_ARN", sqs_invoke.queue_arn)
                lambdaFn.add_event_source(aws_lambda_event_sources.SqsEventSource(sqs_invoke, batch_size=1))

            return lambdaFn

        sns_topic_base = aws_sns.Topic.from_topic_arn(self, "ImportedTopicId", os.environ.get('TOPIC_ARN'))

        # Create S3 bucket
        s3_raw = create_s3(f"{prefix}-01-raw")
        s3_refined = create_s3(f"{prefix}-02-refined")
        s3_enriched = create_s3(f"{prefix}-03-enriched")

        sqs_buffer = create_sqs(f"{prefix}-00-buffer")
        sns_topic_base.add_subscription(aws_sns_subscriptions.SqsSubscription(sqs_buffer))

        # Create invoke SQS
        sqs_01_download = create_fifo_sqs(f"{prefix}-01-download-invoke-02-refine")
        sqs_02_refine = create_fifo_sqs(f"{prefix}-02-refine-invoke-03-enrich")
        sqs_03_enrich = create_fifo_sqs(f"{prefix}-03-enrich-invoke-04-upload")

        # Create Download Lambda
        download_lambda_fn = create_lambda(
            name=f"{prefix}-01-download",
            code=aws_lambda.Code.asset(f"./lambda/01-download"),
            s3_destination=s3_raw,
            sqs_notify=sqs_01_download,
            environment={
                "PREFIX": prefix,
                "SOURCE_SQS_URL": sqs_buffer.queue_url
            }
        )

        aws_events.Rule(
            self, f"Rule-{prefix}-01-download",
            schedule=aws_events.Schedule.rate(Duration.minutes(1)),
            targets=[aws_events_targets.LambdaFunction(download_lambda_fn)]
        )

        sqs_buffer.grant_consume_messages(download_lambda_fn)

        # Create Refine Lambda
        create_lambda(
            name=f"{prefix}-02-refine",
            code=aws_lambda.Code.asset(f"./lambda/02-refine"),
            s3_source=s3_raw,
            s3_destination=s3_refined,
            sqs_invoke=sqs_01_download,
            sqs_notify=sqs_02_refine,
        )

        # Create enrich Lambda
        create_lambda(
            name=f"{prefix}-03-enrich",
            code=aws_lambda.Code.asset(f"./lambda/03-enrich"),
            s3_source=s3_refined,
            s3_destination=s3_enriched,
            sqs_invoke=sqs_02_refine,
            sqs_notify=sqs_03_enrich,
        )

        # Create Upload Lambda
        create_lambda(
            name=f"{prefix}-04-upload",
            code=aws_lambda.Code.asset(f"./lambda/04-upload"),
            s3_source=s3_enriched,
            sqs_invoke=sqs_03_enrich
        )
