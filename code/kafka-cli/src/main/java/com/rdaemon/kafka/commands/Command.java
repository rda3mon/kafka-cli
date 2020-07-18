package com.rdaemon.kafka.commands;

import java.util.List;

public interface Command {
  public void list(List<String> topic);
}
