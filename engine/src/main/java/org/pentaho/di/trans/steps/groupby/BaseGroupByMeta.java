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

  protected GroupByType[] aggregateFunctions;

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
  // TODO: look into making this just GroupByTypes
  protected String[] aggregateType;

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
  public String[] getAggregateType() {
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
    aggregateType = new String[ nrfields ];
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

  protected void readData( Node stepnode ) throws KettleXMLException {
    try {
      Node groupn = XMLHandler.getSubNode( stepnode, "group" );
      Node fields = XMLHandler.getSubNode( stepnode, "fields" );

      int sizegroup = XMLHandler.countNodes( groupn, "field" );
      int nrfields = XMLHandler.countNodes( fields, "field" );

      allocate( sizegroup, nrfields );

      for ( int i = 0; i < sizegroup; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( groupn, "field", i );
        groupField[ i ] = XMLHandler.getTagValue( fnode, "name" );
      }

      boolean hasNumberOfValues = false;
      for ( int i = 0; i < nrfields; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( fields, "field", i );
        aggregateField[ i ] = XMLHandler.getTagValue( fnode, "aggregate" );
        subjectField[ i ] = XMLHandler.getTagValue( fnode, "subject" );
        aggregateType[ i ] = XMLHandler.getTagValue( fnode, "type" );

        if ( aggregateType[ i ].equals( GroupByType.COUNT_ALL.getTypeGroupCode() )
            || aggregateType[ i ].equals( GroupByType.COUNT_DISTINCT.getTypeGroupCode() )
            || aggregateType[ i ].equals( GroupByType.COUNT_ANY.getTypeGroupCode() ) ) {
          hasNumberOfValues = true;
        }

        valueField[ i ] = XMLHandler.getTagValue( fnode, "valuefield" );
      }

      String giveBackRow = XMLHandler.getTagValue( stepnode, "give_back_row" );
      if ( Utils.isEmpty( giveBackRow ) ) {
        alwaysGivingBackOneRow = hasNumberOfValues;
      } else {
        alwaysGivingBackOneRow = "Y".equalsIgnoreCase( giveBackRow );
      }
    } catch ( Exception e ) {
      throw new KettleXMLException( BaseMessages.getString(
          PKG, "GroupByMeta.Exception.UnableToLoadStepInfoFromXML" ), e );
    }
  }

  public final String getTypeDescLong( int i ) {
    if ( i < 0 || i >= aggregateFunctions.length ) {
      return null;
    }

    return aggregateFunctions[ i ].getLongDesc();
  }

  @Override
  public void setDefault() {
    int sizeGroup = 0;
    int numberOfFields = 0;

    allocate( sizeGroup, numberOfFields );
  }

  @Override
  public void getFields( RowMetaInterface rowMeta, String origin, RowMetaInterface[] info, StepMeta nextStep,
                         VariableSpace space, Repository repository, IMetaStore metaStore ) {
    getFields( rowMeta, origin, info, nextStep, space, repository, metaStore, false );
  }

  protected void getFields( RowMetaInterface rowMeta, String origin, RowMetaInterface[] info, StepMeta nextStep,
                            VariableSpace space, Repository repository, IMetaStore metaStore, boolean passAllRows ) {
    // re-assemble a new row of metadata
    //
    RowMetaInterface fields = new RowMeta();

    if ( !passAllRows ) {
      // Add the grouping fields in the correct order...
      //
      for ( int i = 0; i < groupField.length; i++ ) {
        ValueMetaInterface valueMeta = rowMeta.searchValueMeta( groupField[ i ] );
        if ( valueMeta != null ) {
          fields.addValueMeta( valueMeta );
        }
      }
    } else {
      // Add all the original fields from the incoming row meta
      //
      fields.addRowMeta( rowMeta );
    }

    // Re-add aggregates
    //
    for ( int i = 0; i < subjectField.length; i++ ) {
      ValueMetaInterface subj = rowMeta.searchValueMeta( subjectField[ i ] );
      if ( subj != null || aggregateType[ i ].equals( GroupByType.COUNT_ANY.getTypeGroupCode() ) ) {
        String valueName = aggregateField[ i ];
        int valueType = ValueMetaInterface.TYPE_NONE;
        int length = -1;
        int precision = -1;

        GroupByType aggType = GroupByType.getTypeFromString( aggregateType[ i ] );
        switch ( aggType ) {
          case SUM:
          case AVERAGE:
          case CUMULATIVE_SUM:
          case CUMULATIVE_AVERAGE:
          case FIRST:
          case LAST:
          case FIRST_INCL_NULL:
          case LAST_INCL_NULL:
          case MIN:
          case MAX:
            valueType = subj.getType();
            break;
          case COUNT_DISTINCT:
          case COUNT_ANY:
          case COUNT_ALL:
            valueType = ValueMetaInterface.TYPE_INTEGER;
            break;
          case CONCAT_COMMA:
            valueType = ValueMetaInterface.TYPE_STRING;
            break;
          case STANDARD_DEVIATION:
          case MEDIAN:
          case PERCENTILE:
          case STANDARD_DEVIATION_SAMPLE:
            valueType = ValueMetaInterface.TYPE_NUMBER;
            break;
          case CONCAT_STRING:
            valueType = ValueMetaInterface.TYPE_STRING;
            break;
          default:
            break;
        }

        // Change type from integer to number in case off averages for cumulative average
        //
        if ( aggregateType[ i ] == GroupByType.CUMULATIVE_AVERAGE.getTypeGroupCode()
            && valueType == ValueMetaInterface.TYPE_INTEGER ) {
          valueType = ValueMetaInterface.TYPE_NUMBER;
          precision = -1;
          length = -1;
        } else if ( aggregateType[ i ] == GroupByType.COUNT_ALL.getTypeGroupCode()
            || aggregateType[ i ] == GroupByType.COUNT_DISTINCT.getTypeGroupCode()
            || aggregateType[ i ] == GroupByType.COUNT_ANY.getTypeGroupCode() ) {
          length = ValueMetaInterface.DEFAULT_INTEGER_LENGTH;
          precision = 0;
        } else if ( aggregateType[ i ] == GroupByType.SUM.getTypeGroupCode()
            && valueType != ValueMetaInterface.TYPE_INTEGER && valueType != ValueMetaInterface.TYPE_NUMBER
            && valueType != ValueMetaInterface.TYPE_BIGNUMBER ) {
          // If it ain't numeric, we change it to Number
          //
          valueType = ValueMetaInterface.TYPE_NUMBER;
          precision = -1;
          length = -1;
        }

        if ( valueType != ValueMetaInterface.TYPE_NONE ) {
          ValueMetaInterface v;
          try {
            v = ValueMetaFactory.createValueMeta( valueName, valueType );
          } catch ( KettlePluginException e ) {
            v = new ValueMetaNone( valueName );
          }
          v.setOrigin( origin );
          v.setLength( length, precision );

          if ( subj != null ) {
            v.setConversionMask( subj.getConversionMask() );
          }

          fields.addValueMeta( v );
        }
      }
    }

    // Now that we have all the fields we want, we should clear the original row and replace the values...
    //
    rowMeta.clear();
    rowMeta.addRowMeta( fields );
  }

  @Override
  public String getXML() {
    StringBuilder retval = new StringBuilder( 500 );

    retval.append( "      " ).append( XMLHandler.addTagValue( "give_back_row", alwaysGivingBackOneRow ) );

    retval.append( "      <group>" ).append( Const.CR );
    for ( int i = 0; i < groupField.length; i++ ) {
      retval.append( "        <field>" ).append( Const.CR );
      retval.append( "          " ).append( XMLHandler.addTagValue( "name", groupField[i] ) );
      retval.append( "        </field>" ).append( Const.CR );
    }
    retval.append( "      </group>" ).append( Const.CR );

    retval.append( "      <fields>" ).append( Const.CR );
    for ( int i = 0; i < subjectField.length; i++ ) {
      retval.append( "        <field>" ).append( Const.CR );
      retval.append( "          " ).append( XMLHandler.addTagValue( "aggregate", aggregateField[i] ) );
      retval.append( "          " ).append( XMLHandler.addTagValue( "subject", subjectField[i] ) );
      retval.append( "          " ).append( XMLHandler.addTagValue( "type", aggregateType[i] ) );
      retval.append( "          " ).append( XMLHandler.addTagValue( "valuefield", valueField[i] ) );
      retval.append( "        </field>" ).append( Const.CR );
    }
    retval.append( "      </fields>" ).append( Const.CR );

    return retval.toString();
  }

  @Override
  public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases )
      throws KettleException {
    try {
      int groupsize = rep.countNrStepAttributes( id_step, "group_name" );
      int nrvalues = rep.countNrStepAttributes( id_step, "aggregate_name" );

      allocate( groupsize, nrvalues );

      for ( int i = 0; i < groupsize; i++ ) {
        groupField[ i ] = rep.getStepAttributeString( id_step, i, "group_name" );
      }

      boolean hasNumberOfValues = false;
      for ( int i = 0; i < nrvalues; i++ ) {
        aggregateField[ i ] = rep.getStepAttributeString( id_step, i, "aggregate_name" );
        subjectField[ i ] = rep.getStepAttributeString( id_step, i, "aggregate_subject" );
        aggregateType[ i ] = rep.getStepAttributeString( id_step, i, "aggregate_type" );

        if ( aggregateType[ i ] == GroupByType.COUNT_ALL.getTypeGroupCode()
            || aggregateType[ i ] == GroupByType.COUNT_DISTINCT.getTypeGroupCode()
            || aggregateType[ i ] == GroupByType.COUNT_ANY.getTypeGroupCode() ) {
          hasNumberOfValues = true;
        }
        valueField[ i ] = rep.getStepAttributeString( id_step, i, "aggregate_value_field" );
      }

      alwaysGivingBackOneRow = rep.getStepAttributeBoolean( id_step, 0, "give_back_row", hasNumberOfValues );
    } catch ( Exception e ) {
      throw new KettleException( BaseMessages.getString(
          PKG, "BaseGroupByMeta.Exception.UnexpectedErrorInReadingStepInfoFromRepository" ), e );
    }
  }

  @Override
  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step )
      throws KettleException {
    try {
      rep.saveStepAttribute( id_transformation, id_step, "give_back_row", alwaysGivingBackOneRow );

      for ( int i = 0; i < groupField.length; i++ ) {
        rep.saveStepAttribute( id_transformation, id_step, i, "group_name", groupField[i] );
      }

      for ( int i = 0; i < subjectField.length; i++ ) {
        rep.saveStepAttribute( id_transformation, id_step, i, "aggregate_name", aggregateField[i] );
        rep.saveStepAttribute( id_transformation, id_step, i, "aggregate_subject", subjectField[i] );
        rep.saveStepAttribute( id_transformation, id_step, i, "aggregate_type", aggregateType[i] );
        rep.saveStepAttribute( id_transformation, id_step, i, "aggregate_value_field", valueField[i] );
      }
    } catch ( Exception e ) {
      throw new KettleException( BaseMessages.getString(
          PKG, "MemoryGroupByMeta.Exception.UnableToSaveStepInfoToRepository" )
          + id_step, e );
    }
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
