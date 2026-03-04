Examples
========

The following examples demonstrate how to use the Kubeflow SDK for
distributed AI training and LLM fine-tuning.

PyTorch & HuggingFace Examples
-------------------------------

.. list-table::
   :header-rows: 1
   :widths: 30 20 25 25

   * - Task
     - Model
     - Dataset
     - Notebook
   * - Local Training
     - CNN
     - MNIST
     - `Open Notebook <https://github.com/kubeflow/trainer/blob/master/examples/local/local-training-mnist.ipynb>`_
   * - Image Classification
     - CNN
     - Fashion MNIST
     - `Open Notebook <https://github.com/kubeflow/trainer/blob/master/examples/pytorch/image-classification/mnist.ipynb>`_
   * - Question Answering
     - DistilBERT
     - SQuAD
     - `Open Notebook <https://github.com/kubeflow/trainer/blob/master/examples/pytorch/question-answering/fine-tune-distilbert.ipynb>`_
   * - Speech Recognition
     - Transformer
     - Speech Commands
     - `Open Notebook <https://github.com/kubeflow/trainer/blob/master/examples/pytorch/speech-recognition/speech-recognition.ipynb>`_
   * - Audio Classification
     - CNN (M5)
     - GTZAN
     - `Open Notebook <https://github.com/kubeflow/trainer/blob/master/examples/pytorch/audio-classification/audio-classification.ipynb>`_

DeepSpeed Examples
------------------

.. list-table::
   :header-rows: 1
   :widths: 30 20 25 25

   * - Task
     - Model
     - Dataset
     - Notebook
   * - Text Summarization
     - T5
     - CNN/DailyMail
     - `Open Notebook <https://github.com/kubeflow/trainer/blob/master/examples/deepspeed/text-summarization/T5-Fine-Tuning.ipynb>`_

MLX Examples
------------

.. list-table::
   :header-rows: 1
   :widths: 30 20 25 25

   * - Task
     - Model
     - Dataset
     - Notebook
   * - Image Classification
     - MLP
     - MNIST
     - `Open Notebook <https://github.com/kubeflow/trainer/blob/master/examples/mlx/image-classification/mnist.ipynb>`_
   * - LLM Fine-Tuning
     - Llama 3.2-3B
     - WikiSQL
     - `Open Notebook <https://github.com/kubeflow/trainer/blob/master/examples/mlx/language-modeling/fine-tune-llama.ipynb>`_

TorchTune Examples
------------------

.. list-table::
   :header-rows: 1
   :widths: 30 20 25 25

   * - Task
     - Model
     - Dataset
     - Notebook
   * - LLM Fine-Tuning
     - Llama 3.2-1B
     - Alpaca
     - `Open Notebook <https://github.com/kubeflow/trainer/blob/master/examples/torchtune/llama3_2/alpaca-trainjob-yaml.ipynb>`_

Feast Feature Store Examples
----------------------------

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Example
     - Description
   * - `Quickstart <https://github.com/kubeflow/sdk/blob/main/examples/feast/feast_quickstart.py>`_
     - Deploy a feature store, retrieve online features
   * - `Deploy & Serve <https://github.com/kubeflow/sdk/blob/main/examples/feast/feast_deploy_and_serve.py>`_
     - Production deployment with Redis, CronJob, and replicas
   * - `Historical Features <https://github.com/kubeflow/sdk/blob/main/examples/feast/feast_historical_features.py>`_
     - Retrieve historical features for model training
