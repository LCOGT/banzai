#!/usr/bin/env groovy

@Library('lco-shared-libs@0.0.1') _

pipeline {
	agent any
	environment {
		dockerImage = null
		PROJ_NAME = projName("${JOB_NAME}")
		GIT_DESCRIPTION = gitDescription()
		DOCKER_IMG = dockerImageName("${LCO_DOCK_REG}", "${PROJ_NAME}", "${GIT_DESCRIPTION}")
	}
	options {
		timeout(time: 1, unit: 'HOURS')
	}
	stages {
		stage('Build image') {
			steps {
				script {
					dockerImage = docker.build("${DOCKER_IMG}", "--build-arg MINICONDA_VERSION=4.4.10 .")
				}
			}
		}
		stage('Push image') {
			steps {
				script {
					dockerImage.push("${GIT_DESCRIPTION}")
				}
			}
		}
		stage('Test') {
			steps {
				script {
					sh 'docker run --rm -w=/lco/banzai/ --user=root "${DOCKER_IMG}" python setup.py test'
				}
			}
		}
	}
}
