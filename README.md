# UOCC Skid

[![Push Events](https://github.com/agrc/uocc-skid/actions/workflows/push.yml/badge.svg)](https://github.com/agrc/uocc-skid/actions/workflows/push.yml)

Moves data around to support a Survey123 form to replace DEQ's Used Oil Collection Center (UOCC) PDF forms.

This has two distinct ETL pipelines: Google Sheets to feature service to pre-populate form data and feature service to Google Sheets to extract form responses.

## 1: Sheets to Feature Service

DEQ maintains two sheets/tabs of information that need to be put into a feature service so that the Survey123 form can pull them in.

## 2: Feature Service to Sheets

The Survey123 results feature service should be put into multiple sheets: one sheet that holds all the results, and an individual sheet for each LHD containing just their data.

## Schedule

The skid should be run weekly, every Sunday evening.

