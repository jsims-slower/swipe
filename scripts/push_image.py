#!/usr/bin/env python3

import argparse
import subprocess
import warnings

import os
import sys
import json
import logging
import datetime


def docker_login(profile: str):
    account = run_cmd(
        ["aws", "sts", "get-caller-identity", "--profile", profile, "--query", "Account", "--output", "text"])
    if not account:
        raise Exception("AWS Profile Has no Account")
    print(f"account = {account}")

    password = run_cmd(["aws", "ecr", "get-login-password", "--region", "us-west-2", "--profile", profile])
    print(f"password = {password}")

    docker_url = f"{account}.dkr.ecr.us-west-2.amazonaws.com"
    print(f"docker_url = {docker_url}")

    res = run_cmd(["docker", "login", "--username", "AWS", "--password-stdin", docker_url], password)
    print(f"Docker Login: {res}")

    return docker_url


def run_cmd(cmd, input=None):
    print(' '.join(cmd))
    try:
        if input is None:
            p = subprocess.run(cmd, encoding='utf-8', stdout=subprocess.PIPE)
        else:
            p = subprocess.run(cmd, input=input, encoding='utf-8', stdout=subprocess.PIPE)
        if p.returncode != 0:
            raise Exception("Command failed!")
        return p.stdout.strip()
    except Exception as ex:
        warnings.warn(ex)
        raise


def main():
    parser = argparse.ArgumentParser(description="Build and deploy Docker Image to ERC")
    parser.add_argument("-p", "--profile", type=str, help="AWS Profile", required=True)
    # parser.add_argument( "-v", "--verbose", help="Enable verbose output", action="store_true" )
    args = parser.parse_args()

    profile = args.profile
    if not profile:
        raise Exception("AWS Profile Required")

    docker_url = docker_login(args.profile)
    module = "swipe"
    # version="latest"

    # --platform linux/amd64,linux/arm64
    cmd = ["docker", "buildx", "build", "--platform", "linux/amd64", ".", "--tag", f"{docker_url}/{module}:latest"]
    cmd += ["--push"]
    print(' '.join(cmd))

    # run_cmd(cmd)


if __name__ == "__main__":
    main()
