#!/bin/bash

ps aux | grep hadoop | grep compose | awk '{print $2}' | xargs kill -9