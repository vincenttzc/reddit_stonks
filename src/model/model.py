from typing import Tuple
import random

import numpy as np
import pandas as pd
import torch
from torch.utils.data import DataLoader
from transformers import AutoModel, AutoTokenizer, AutoModelForSequenceClassification


# Set seed
torch.manual_seed(0)
random.seed(0)
np.random.seed(0)


class Model:
    """Model class to create a sequence classification model from huggingface

    Args:
        model_name (str): model name, a valid model from huggingface
        batch_size (int): batch size for prediction
    """

    def __init__(self, model_name: str, batch_size: int):
        self.model = AutoModelForSequenceClassification.from_pretrained(model_name)
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.batch_size = batch_size

    def predict(self, text: list) -> list:
        """Predict sentiment using hugging face model from a list of text

        Args:
            text (list): list of texts to perform sentiment analysis

        Returns:
            list: list of sentiment, 1=positive 0=negative
        """

        encoded_input = self.__encode_token(text)
        dataloaders = self.__create_dataloaders(encoded_input, self.batch_size)

        preds = []
        for i, batch in enumerate(zip(dataloaders[0], dataloaders[1], dataloaders[2])):
            preds_batch_list = self.__predict_batch(self.model, batch)
            preds += preds_batch_list

        return preds

    def __encode_token(self, text: list) -> dict:
        """Encode list of texts into tokens, which is input needed for
        huggingface model

        Args:
            text (list): list of texts to encode

        Returns:
            dict: dict of encoded tokens
        """
        encoded_input = self.tokenizer(
            [str(string) for string in text],
            truncation=True,
            padding=True,
            return_tensors="pt",
        )

        return encoded_input

    def __create_dataloaders(
        self, encoded_input: dict, batch_size
    ) -> Tuple[DataLoader, DataLoader, DataLoader]:
        """Create dataloaders for making batch prediction

        Args:
            encoded_input (dict): dict of encoded tokens
            batch_size ([type]): batch size of dataloader

        Returns:
            Tuple[DataLoader, DataLoader, DataLoader]: Dataloader of each token
        """
        input_ids = encoded_input["input_ids"]
        token_type_ids = encoded_input["token_type_ids"]
        attention_mask = encoded_input["attention_mask"]

        input_ids_data_loader = torch.utils.data.DataLoader(
            input_ids, batch_size=batch_size, shuffle=False
        )
        token_type_ids_data_loader = torch.utils.data.DataLoader(
            token_type_ids, batch_size=batch_size, shuffle=False
        )
        attention_mask_data_loader = torch.utils.data.DataLoader(
            attention_mask, batch_size=batch_size, shuffle=False
        )

        return (
            input_ids_data_loader,
            token_type_ids_data_loader,
            attention_mask_data_loader,
        )

    def __predict_batch(self, model: AutoModel, batch: Tuple):
        """Make a batch inference using huggingface model

        Args:
            model (transformers.PreTrainedModel): model for making prediction
            batch (Tuple): tuple of dataloaders

        Returns:
            [type]: list of sentiment
        """
        input_ids_batch = batch[0]
        token_type_ids_batch = batch[1]
        attention_mask_batch = batch[2]

        output = model(
            input_ids=input_ids_batch,
            token_type_ids=token_type_ids_batch,
            attention_mask=attention_mask_batch,
        )

        logits = output.logits
        preds_batch = np.argmax(torch.softmax(logits, dim=1).detach().numpy(), axis=1)
        preds_batch_list = list(preds_batch)

        return preds_batch_list


if __name__ == "__main__":
    model = Model("albert-base-v2", 2)
    texts = [
        "hello how are you?",
        "i am good",
        "how is your day?",
        "the weather is fine",
    ]
    preds = model.predict(texts)
    print(preds)
