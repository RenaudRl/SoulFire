/*
 * SoulFire
 * Copyright (C) 2026  AlexProgrammerDE
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package com.soulfiremc.server.task;

import com.soulfiremc.grpc.generated.ContainerTransferDirection;
import com.soulfiremc.grpc.generated.ContainerTransferOperation;
import com.soulfiremc.grpc.generated.ContainerTransferTask;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

class ContainerTransferInputTest {
  @ParameterizedTest
  @ValueSource(ints = {0, 1_000_001, Integer.MAX_VALUE, Integer.MIN_VALUE, -1})
  void rejectsInvalidUnsignedCountsBeforeReadingTheBot(int count) {
    var context = mock(BotTaskContext.class);
    var input = ContainerTransferTask.newBuilder()
      .setDirection(ContainerTransferDirection.CONTAINER_TRANSFER_DIRECTION_WITHDRAW)
      .addOperations(ContainerTransferOperation.newBuilder().setCount(count))
      .build();
    var error = assertThrows(StatusRuntimeException.class, () -> new ContainerTransferTaskProvider().start(context, input));
    assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    verifyNoInteractions(context);
  }
}
