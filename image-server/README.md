# Serverless IIIF Deployment (AWS)

This document describes how to deploy a Serverless IIIF Image Service on AWS using Serverless IIIF and CloudFormation. The service is fronted by CloudFront on a custom domain (iiif-image.juncture-digital.io) with automatic image expiration via S3 lifecycle policies.

--- 

## Prerequisites

- AWS account with permissions for S3, Lambda, CloudFront, ACM, and CloudFormation
- AWS CLI configured with credentials
- An ACM certificate for your custom domain (iiif-image.juncture-digital.io or *.juncture-digital.io) in us-east-1
- Domain managed in Squarespace (or another DNS provider) where you can add a CNAME record

---

## Steps

### 1. Create the S3 bucket for source images

```bash
aws s3api create-bucket --bucket juncture-images --region us-east-1
```

Add a lifecycle rule to automatically expire images after 90 days:

```bash
aws s3api put-bucket-lifecycle-configuration \
  --bucket juncture-images \
  --lifecycle-configuration '{
    "Rules": [
      {
        "ID": "ExpireAfter90Days",
        "Filter": { "Prefix": "" },
        "Status": "Enabled",
        "Expiration": { "Days": 90 },
        "AbortIncompleteMultipartUpload": { "DaysAfterInitiation": 7 }
      }
    ]
  }'
```

Upload test images:

```bash
aws s3 cp test.tif s3://juncture-images/
```

---

### 2. Deploy the CloudFormation stack

Get the cert arn

```bash
aws acm list-certificates \
  --region us-east-1 \
  --query "CertificateSummaryList[?DomainName=='iiif-image.juncture-digital.io' || DomainName=='*.juncture-digital.io'].CertificateArn" \
  --output text
```

Use the provided template (serverless-iiif-image.yaml) that:
- Installs the Serverless IIIF Lambda via the AWS Serverless Application Repository (SAR)
- Creates a CloudFront distribution with custom domain support
- Forwards query strings and Accept headers to the origin
- Applies permissive CORS headers

Deploy:

```bash
aws cloudformation deploy \
  --region us-east-1 \
  --template-file serverless-iiif-image.yaml \
  --stack-name iiif-image \
  --capabilities CAPABILITY_IAM CAPABILITY_AUTO_EXPAND \
  --parameter-overrides \
    DomainName=iiif-image.juncture-digital.io \
    CertificateArn=<your-acm-cert-arn> \
    SourceBucket=juncture-images \
    SharpLayer=JP2 \
    IiifLambdaMemory=3008 \
    IiifLambdaTimeout=10 \
    ForceHost=iiif-image.juncture-digital.io
```

---

### 3. Configure DNS
- In Squarespace DNS, add a CNAME record:
- Host: iiif-image
- Value: <CloudFrontDomainName> (from CloudFormation Outputs, e.g. dxxxxxx.cloudfront.net)

---

### 4. Test the service

Replace <id> with your object key in S3 (URL-encode / if present):

Get IIIF info.json

```bash
curl -sS https://iiif-image.juncture-digital.io/iiif/2/<id>/info.json | jq .
```

Request a derivative image

```bash
curl -I "https://iiif-image.juncture-digital.io/iiif/2/<id>/full/600,/0/default.jpg"
```
You should receive JSON metadata and a JPEG response.

### 5. Cleanup / migration

If replacing an old bucket (e.g., mdpress-images):
  1.	Create juncture-images with lifecycle rules.
  2.	Copy objects created in the last 90 days.
  3.	Update CloudFormation to point to juncture-images.
  4.	Delete the old bucket:

```bash
aws s3 rb s3://mdpress-images --force
```

---

Notes
- Lifecycle expiration is based on object creation/modification date, not last access.
- For CloudFront caching, invalidations can be created if needed:

```bash
aws cloudfront create-invalidation --distribution-id <DIST_ID> --paths "/iiif/*"
```

- The stack uses the published Serverless IIIF Lambda (SemanticVersion: 5.1.7) from SAR.
