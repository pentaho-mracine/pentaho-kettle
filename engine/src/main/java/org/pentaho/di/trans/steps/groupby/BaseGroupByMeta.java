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

import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.row.value.ValueMetaInteger;
import org.pentaho.di.core.row.value.ValueMetaNone;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.List;

@InjectionSupported( localizationPrefix = "BaseGroupBy.Injection.", groups = { "FIELDS", "AGGREGATES" } )
abstract public class BaseGroupByMeta extends BaseStepMeta implements StepMetaInterface {
  private static Class<?> PKG = BaseGroupByMeta.class; // for i18n purposes, needed by Translator2!!

  public static final int TYPE_GROUP_NONE = 0;
  public static final int TYPE_GROUP_SUM = 1;
  public static final int TYPE_GROUP_AVERAGE = 2;
  public static final int TYPE_GROUP_MEDIAN = 3;
  public static final int TYPE_GROUP_PERCENTILE = 4;
  public static final int TYPE_GROUP_MIN = 5;
  public static final int TYPE_GROUP_MAX = 6;
  public static final int TYPE_GROUP_COUNT_ALL = 7;
  public static final int TYPE_GROUP_CONCAT_COMMA = 8;
  public static final int TYPE_GROUP_FIRST = 9;
  public static final int TYPE_GROUP_LAST = 10;
  public static final int TYPE_GROUP_FIRST_INCL_NULL = 11;
  public static final int TYPE_GROUP_LAST_INCL_NULL = 12;
  public static final int TYPE_GROUP_CUMULATIVE_SUM = 13;
  public static final int TYPE_GROUP_CUMULATIVE_AVERAGE = 14;
  public static final int TYPE_GROUP_STANDARD_DEVIATION = 15;
  public static final int TYPE_GROUP_CONCAT_STRING = 16;
  public static final int TYPE_GROUP_COUNT_DISTINCT = 17;
  public static final int TYPE_GROUP_COUNT_ANY = 18;
  public static final int TYPE_GROUP_STANDARD_DEVIATION_SAMPLE = 19;
  public static final int TYPE_GROUP_PERCENTILE_NEAREST_RANK = 20;

  public static final String[] typeGroupCode = /* WARNING: DO NOT TRANSLATE THIS. WE ARE SERIOUS, DON'T TRANSLATE! */
      {
          "-", "SUM", "AVERAGE", "MEDIAN", "PERCENTILE", "MIN", "MAX", "COUNT_ALL", "CONCAT_COMMA", "FIRST", "LAST",
          "FIRST_INCL_NULL", "LAST_INCL_NULL", "CUM_SUM", "CUM_AVG", "STD_DEV", "CONCAT_STRING", "COUNT_DISTINCT",
          "COUNT_ANY", "STD_DEV_SAMPLE", "PERCENTILE_NEAREST_RANK" };

  public static final String[] typeGroupLongDesc = {
      "-", BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.SUM" ),
      BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.AVERAGE" ),
      BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.MEDIAN" ),
      BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.PERCENTILE" ),
      BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.MIN" ),
      BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.MAX" ),
      BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.CONCAT_ALL" ),
      BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.CONCAT_COMMA" ),
      BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.FIRST" ),
      BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.LAST" ),
      BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.FIRST_INCL_NULL" ),
      BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.LAST_INCL_NULL" ),
      BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.CUMUMALTIVE_SUM" ),
      BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.CUMUMALTIVE_AVERAGE" ),
      BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.STANDARD_DEVIATION" ),
      BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.CONCAT_STRING" ),
      BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.COUNT_DISTINCT" ),
      BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.COUNT_ANY" ),
      BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.STANDARD_DEVIATION_SAMPLE" ),
      BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.PERCENTILE_NEAREST_RANK" )
  };

  /**
   * Fields to group over
   */
  @Injection( name = "GROUPFIELD", group = "FIELDS" )
  protected String[] groupField;

  /**
   * Name of aggregate field
   */
  @Injection( name = "AGGREGATEFIELD", group = "AGGREGATES" )
  protected String[] aggregateField;

  /**
   * Field name to group over
   */
  @Injection( name = "SUBJECTFIELD", group = "AGGREGATES" )
  protected String[] subjectField;

  /**
   * Type of aggregate
   */
  @Injection( name="AGGREGATETYPE", group = "AGGREGATES" )
  protected int[] aggregateType;

  /**
   * Value to use as separator for ex
   */
  @Injection( name = "VALUEFIELD", group = "AGGREGATES" )
  protected String[] valueField;

  /**
   * Flag to indicate that we always give back one row. Defaults to true for existing transformations.
   */
  @Injection( name = "ALWAYSGIVINGBACKONEROW", group = "FIELDS" )
  protected boolean alwaysGivingBackOneRow;

  public BaseGroupByMeta() {
    super(); // allocate BaseStepMeta
  }

  /**
   * @return Returns the aggregateField.
   */
  public String[] getAggregateField() {
    return aggregateField;
  }

  /**
   * @param aggregateField The aggregateField to set.
   */
  public void setAggregateField( String[] aggregateField ) {
    this.aggregateField = aggregateField;
  }

  /**
   * @return Returns the aggregateType.
   */
  public int[] getAggregateType() {
    return aggregateType;
  }

  /**
   * @param aggregateType The aggregateType to set.
   */
  public void setAggregateType( int[] aggregateType ) {
    this.aggregateType = aggregateType;
  }

  /**
   * @return Returns the groupField.
   */
  public String[] getGroupField() {
    return groupField;
  }

  /**
   * @param groupField The groupField to set.
   */
  public void setGroupField( String[] groupField ) {
    this.groupField = groupField;
  }

  /**
   * @return Returns the subjectField.
   */
  public String[] getSubjectField() {
    return subjectField;
  }

  /**
   * @param subjectField The subjectField to set.
   */
  public void setSubjectField( String[] subjectField ) {
    this.subjectField = subjectField;
  }

  /**
   * @return Returns the valueField.
   */
  public String[] getValueField() {
    return valueField;
  }

  /**
   * @param valueField The valueField to set.
   */
  public void setValueField( String[] valueField ) {
    this.valueField = valueField;
  }

  @Override
  public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    readData( stepnode );
  }

  public void allocate( int sizegroup, int nrfields ) {
    groupField = new String[ sizegroup ];
    aggregateField = new String[ nrfields ];
    subjectField = new String[ nrfields ];
    aggregateType = new int[ nrfields ];
    valueField = new String[ nrfields ];
  }

  @Override
  public Object clone() {
    BaseGroupByMeta retval = (BaseGroupByMeta) super.clone();

    int szGroup = 0, szFields = 0;
    if ( groupField != null ) {
      szGroup = groupField.length;
    }

    if ( valueField != null ) {
      szFields = valueField.length;
    }

    retval.allocate( szGroup, szFields );

    System.arraycopy( groupField, 0, retval.groupField, 0, szGroup );
    System.arraycopy( aggregateField, 0, retval.aggregateField, 0, szFields );
    System.arraycopy( subjectField, 0, retval.subjectField, 0, szFields );
    System.arraycopy( aggregateType, 0, retval.aggregateType, 0, szFields );
    System.arraycopy( valueField, 0, retval.valueField, 0, szFields );

    return retval;
  }

  public static final int getType( String desc ) {
    for ( int i = 0; i < typeGroupCode.length; i++ ) {
      if ( typeGroupCode[ i ].equalsIgnoreCase( desc ) ) {
        return i;
      }
    }

    for ( int i = 0; i < typeGroupLongDesc.length; i++ ) {
      if ( typeGroupLongDesc[ i ].equalsIgnoreCase( desc ) ) {
        return i;
      }
    }

    return 0;
  }

  public static final String getTypeDesc( int i ) {
    if ( i < 0 || i >= typeGroupCode.length ) {
      return null;
    }

    return typeGroupCode[ i ];
  }

  public static final String getTypeDescLong( int i ) {
    if ( i < 0 || i >= typeGroupLongDesc.length ) {
      return null;
    }

    return typeGroupLongDesc[ i ];
  }

  @Override
  public void setDefault() {
    int sizeGroup = 0;
    int numberOfFields = 0;

    allocate( sizeGroup, numberOfFields );
  }

  @Override
  public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta,
                     RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
                     Repository repository, IMetaStore metaStore ) {
    CheckResult cr;

    if ( input.length > 0 ) {
      cr =
          new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
              PKG, "BaseGroupByMeta.CheckResult.ReceivingInfoOK" ), stepMeta );
      remarks.add( cr );
    } else {
      cr =
          new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
              PKG, "BaseGroupByMeta.CheckResult.NoInputError" ), stepMeta );
      remarks.add( cr );
    }
  }

  abstract public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface,
                                         int cnr, TransMeta transMeta, Trans trans );

  abstract public StepDataInterface getStepData();

  /**
   * @return the alwaysGivingBackOneRow
   */
  public boolean isAlwaysGivingBackOneRow() {
    return alwaysGivingBackOneRow;
  }

  /**
   * @param alwaysGivingBackOneRow the alwaysGivingBackOneRow to set
   */
  public void setAlwaysGivingBackOneRow( boolean alwaysGivingBackOneRow ) {
    this.alwaysGivingBackOneRow = alwaysGivingBackOneRow;
  }

  @Override
  public TransMeta.TransformationType[] getSupportedTransformationTypes() {
    return new TransMeta.TransformationType[] { TransMeta.TransformationType.Normal };
  }
}
