package com.datatorrent.demos.telcom;

import java.io.File;

import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;

public class HdfsBytesOutputOperator  extends AbstractFileOutputOperator<byte[]>
{
  private transient String outputFileName;
  private transient String contextId;
  private int index = 0;

  public HdfsBytesOutputOperator()
  {
    setMaxLength(1024 * 1024);
  }

  @Override
  public void setup(OperatorContext context)
  {
    contextId = context.getValue(DAGContext.APPLICATION_NAME);
    outputFileName = File.separator + contextId +
                     File.separator + "transactions.out.part";
    super.setup(context);
  }

  @Override
  public byte[] getBytesForTuple(byte[] t)
  {
    return t;
  }

  @Override
  protected String getFileName(byte[] tuple)
  {
    return outputFileName;
  }

  @Override
  public String getPartFileName(String fileName, int part)
  {
    return fileName + part;
  }
}