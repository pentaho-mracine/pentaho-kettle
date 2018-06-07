/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.trans.steps.groupby;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;

abstract public class BaseGroupBy extends BaseStep implements StepInterface {

  protected BaseGroupByData data;

  protected boolean allNullsAreZero = false;
  protected boolean minNullIsValued = false;

  public BaseGroupBy( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
                      Trans trans ){
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  protected void initGroupMeta( RowMetaInterface previousRowMeta ) throws KettleValueException {
    data.groupMeta = new RowMeta();
    for ( int i = 0; i < data.groupnrs.length; i++ ) {
      ValueMetaInterface valueMeta = previousRowMeta.getValueMeta( data.groupnrs[ i ] );
      data.groupMeta.addValueMeta( valueMeta );
    }
  }

  protected abstract void handleLastOfGroup() throws KettleException;

  @Override
  public void batchComplete() throws KettleException {
    handleLastOfGroup();
    data.newBatch = true;
  }

  /**
   * Used for junits in GroupByAggregationNullsTest
   *
   * @param allNullsAreZero the allNullsAreZero to set
   */
  void setAllNullsAreZero( boolean allNullsAreZero ) {
    this.allNullsAreZero = allNullsAreZero;
  }

  /**
   * Used for junits in GroupByAggregationNullsTest
   *
   * @param minNullIsValued the minNullIsValued to set
   */
  void setMinNullIsValued( boolean minNullIsValued ) {
    this.minNullIsValued = minNullIsValued;
  }
}
