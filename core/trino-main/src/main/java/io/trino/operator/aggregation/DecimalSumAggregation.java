/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.operator.aggregation;

import com.google.common.collect.ImmutableList;
import io.trino.metadata.AggregationFunctionMetadata;
import io.trino.metadata.BoundSignature;
import io.trino.metadata.FunctionMetadata;
import io.trino.metadata.Signature;
import io.trino.metadata.SqlAggregationFunction;
import io.trino.operator.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import io.trino.operator.aggregation.state.LongDecimalWithOverflowState;
import io.trino.operator.aggregation.state.LongDecimalWithOverflowStateFactory;
import io.trino.operator.aggregation.state.LongDecimalWithOverflowStateSerializer;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.trino.spi.type.Int128Math.addWithOverflow;
import static io.trino.spi.type.TypeSignatureParameter.numericParameter;
import static io.trino.spi.type.TypeSignatureParameter.typeVariable;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.util.Reflection.methodHandle;

public class DecimalSumAggregation
        extends SqlAggregationFunction
{
    public static final DecimalSumAggregation DECIMAL_SUM_AGGREGATION = new DecimalSumAggregation();
    private static final String NAME = "sum";
    private static final MethodHandle SHORT_DECIMAL_INPUT_FUNCTION = methodHandle(DecimalSumAggregation.class, "inputShortDecimal", LongDecimalWithOverflowState.class, Block.class, int.class);
    private static final MethodHandle LONG_DECIMAL_INPUT_FUNCTION = methodHandle(DecimalSumAggregation.class, "inputLongDecimal", LongDecimalWithOverflowState.class, Block.class, int.class);

    private static final MethodHandle LONG_DECIMAL_OUTPUT_FUNCTION = methodHandle(DecimalSumAggregation.class, "outputLongDecimal", LongDecimalWithOverflowState.class, BlockBuilder.class);

    private static final MethodHandle COMBINE_FUNCTION = methodHandle(DecimalSumAggregation.class, "combine", LongDecimalWithOverflowState.class, LongDecimalWithOverflowState.class);

    public DecimalSumAggregation()
    {
        super(
                FunctionMetadata.aggregateBuilder()
                        .signature(Signature.builder()
                                .name(NAME)
                                .returnType(new TypeSignature("decimal", numericParameter(38), typeVariable("s")))
                                .argumentType(new TypeSignature("decimal", typeVariable("p"), typeVariable("s")))
                                .build())
                        .description("Calculates the sum over the input values")
                        .build(),
                AggregationFunctionMetadata.builder()
                        .intermediateType(VARBINARY)
                        .build());
    }

    @Override
    public AggregationMetadata specialize(BoundSignature boundSignature)
    {
        Type inputType = getOnlyElement(boundSignature.getArgumentTypes());
        checkArgument(inputType instanceof DecimalType, "type must be Decimal");
        MethodHandle inputFunction;
        Class<LongDecimalWithOverflowState> stateInterface = LongDecimalWithOverflowState.class;
        LongDecimalWithOverflowStateSerializer stateSerializer = new LongDecimalWithOverflowStateSerializer();

        if (((DecimalType) inputType).isShort()) {
            inputFunction = SHORT_DECIMAL_INPUT_FUNCTION;
        }
        else {
            inputFunction = LONG_DECIMAL_INPUT_FUNCTION;
        }

        return new AggregationMetadata(
                inputFunction,
                Optional.empty(),
                Optional.of(COMBINE_FUNCTION),
                LONG_DECIMAL_OUTPUT_FUNCTION,
                ImmutableList.of(new AccumulatorStateDescriptor<>(
                        stateInterface,
                        stateSerializer,
                        new LongDecimalWithOverflowStateFactory())));
    }

    public static void inputShortDecimal(LongDecimalWithOverflowState state, Block block, int position)
    {
        state.setNotNull();

        long[] decimal = state.getDecimalArray();
        int offset = state.getDecimalArrayOffset();

        long rightLow = block.getLong(position, 0);
        long rightHigh = rightLow >> 63;

        long overflow = addWithOverflow(
                decimal[offset],
                decimal[offset + 1],
                rightHigh,
                rightLow,
                decimal,
                offset);
        state.setOverflow(Math.addExact(overflow, state.getOverflow()));
    }

    public static void inputLongDecimal(LongDecimalWithOverflowState state, Block block, int position)
    {
        state.setNotNull();

        long[] decimal = state.getDecimalArray();
        int offset = state.getDecimalArrayOffset();

        long rightHigh = block.getLong(position, 0);
        long rightLow = block.getLong(position, SIZE_OF_LONG);

        long overflow = addWithOverflow(
                decimal[offset],
                decimal[offset + 1],
                rightHigh,
                rightLow,
                decimal,
                offset);

        state.addOverflow(overflow);
    }

    public static void combine(LongDecimalWithOverflowState state, LongDecimalWithOverflowState otherState)
    {
        long[] decimal = state.getDecimalArray();
        int offset = state.getDecimalArrayOffset();

        long[] otherDecimal = otherState.getDecimalArray();
        int otherOffset = otherState.getDecimalArrayOffset();

        if (state.isNotNull()) {
            long overflow = addWithOverflow(
                    decimal[offset],
                    decimal[offset + 1],
                    otherDecimal[otherOffset],
                    otherDecimal[otherOffset + 1],
                    decimal,
                    offset);
            state.addOverflow(Math.addExact(overflow, otherState.getOverflow()));
        }
        else {
            state.setNotNull();
            decimal[offset] = otherDecimal[otherOffset];
            decimal[offset + 1] = otherDecimal[otherOffset + 1];
            state.setOverflow(otherState.getOverflow());
        }
    }

    public static void outputLongDecimal(LongDecimalWithOverflowState state, BlockBuilder out)
    {
        if (state.isNotNull()) {
            if (state.getOverflow() != 0) {
                throw new ArithmeticException("Decimal overflow");
            }

            long[] decimal = state.getDecimalArray();
            int offset = state.getDecimalArrayOffset();

            long rawHigh = decimal[offset];
            long rawLow = decimal[offset + 1];

            Decimals.throwIfOverflows(rawHigh, rawLow);
            out.writeLong(rawHigh);
            out.writeLong(rawLow);
            out.closeEntry();
        }
        else {
            out.appendNull();
        }
    }
}
