/*
 * Copyright 2018 Amient Ltd, London
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"flag"
	"github.com/amient/goconnect/pkg/goc"
	"github.com/amient/goconnect/pkg/goc/coder"
	"github.com/amient/goconnect/pkg/goc/io"
	"github.com/amient/goconnect/pkg/goc/io/amqp09"
)

var (
	//sink arguments
	uri          = flag.String("amqp-uri", "amqp://guest:guest@localhost:5672/", "AMQP URI")
	exchange     = flag.String("amqp-exchange", "test-exchange", "Durable, non-auto-deleted AMQP exchange name")
	exchangeType = flag.String("amqp-exchange-type", "direct", "Exchange type - direct|fanout|kafkaSinkTopic|x-custom")
	queue        = flag.String("amqp-queue", "test", "Ephemeral AMQP queue name")
	bindingKey   = flag.String("amqp-key", "", "AMQP binding key")

	data = []string{
		`<?xml version="1.0" encoding="utf-8" standalone="no"?><POSLog xmlns="http://www.nrf-arts.org/IXRetail/namespace/" xmlns:r10Ex="http://www.Retalix.com/Extensions" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" FixVersion="1" MajorVersion="6" MinorVersion="0">
  <Transaction FixVersion="1" MajorVersion="6" MinorVersion="0">
    <BusinessUnit>
      <UnitID Name="0610 WELLINGBOROUGH">0610</UnitID>
    </BusinessUnit>
    <WorkstationID TypeCode="POS" WorkstationLocation="3">140</WorkstationID>
    <SequenceNumber>451</SequenceNumber>
    <TransactionID>119EKZ37QD</TransactionID>
    <OperatorID OperatorName="c1eba447a2d360ae0ed716fa6a261b9974525abbfcae9412b06d88cae209ff15" OperatorType="Supervisor" WorkerID="200d2a2e2ce4a65655586a4981529b824f104154c34379e74efa4951d621cc49">0610-9009</OperatorID>
    <ReceiptImage r10Ex:ReceiptFormat="RDR" r10Ex:ReceiptKind="MainReceipt" r10Ex:ReceiptNotPrinted="False">
      <ReceiptLine>&lt;OPOSPrint xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema"&gt;&lt;Documents NumOfDocuments="1" Device="IBM" CodePage="437"&gt;&lt;Document CodePage="437"&gt;&lt;PrintSection Layout="Portrait" SectionID="1"&gt;&lt;PrintCommand Command="PrintStoredBitmap" CmdData="1" /&gt;&lt;TextObject OutputOptions="All"&gt;&lt;TextLine&gt;&lt;LinePrintAttributes Align="Center" CPL="42" /&gt;&lt;Text Text="             WELLINGBOROUGH             "&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;/TextLine&gt;&lt;/TextObject&gt;&lt;TextObject OutputOptions="All"&gt;&lt;TextLine&gt;&lt;LinePrintAttributes Align="Center" CPL="42" /&gt;&lt;Text Text="              01933 443040              "&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;/TextLine&gt;&lt;/TextObject&gt;&lt;TextObject OutputOptions="All"&gt;&lt;TextLine&gt;&lt;LinePrintAttributes Align="Left" CPL="42" /&gt;&lt;Text Text=""&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;/TextLine&gt;&lt;/TextObject&gt;&lt;TextObject OutputOptions="All"&gt;&lt;TextLine&gt;&lt;LinePrintAttributes Align="Center" CPL="42" /&gt;&lt;Text Text="      Sainsbury's Supermarkets Ltd      "&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;/TextLine&gt;&lt;/TextObject&gt;&lt;TextObject OutputOptions="All"&gt;&lt;TextLine&gt;&lt;LinePrintAttributes Align="Center" CPL="42" /&gt;&lt;Text Text="       33 Holborn London EC1N 2HT       "&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;/TextLine&gt;&lt;/TextObject&gt;&lt;TextObject OutputOptions="All"&gt;&lt;TextLine&gt;&lt;LinePrintAttributes Align="Center" CPL="42" /&gt;&lt;Text Text="          www.sainsburys.co.uk          "&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;/TextLine&gt;&lt;/TextObject&gt;&lt;TextObject OutputOptions="All"&gt;&lt;TextLine&gt;&lt;LinePrintAttributes Align="Center" CPL="42" /&gt;&lt;Text Text="        Vat Number : 660 4548 36        "&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;/TextLine&gt;&lt;/TextObject&gt;&lt;TextObject OutputOptions="All"&gt;&lt;TextLine&gt;&lt;LinePrintAttributes Align="Left" CPL="42" /&gt;&lt;Text Text=""&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;/TextLine&gt;&lt;/TextObject&gt;&lt;/PrintSection&gt;&lt;PrintSection Layout="Portrait" SectionID="Reprint" /&gt;&lt;PrintSection Layout="Portrait" SectionID="2"&gt;&lt;TextObject OutputOptions="All"&gt;&lt;TextLine&gt;&lt;LinePrintAttributes Align="Left" CPL="42" /&gt;&lt;Text Text="    "&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;Text Text="YRKSH PRK/CHLLI BRGR "&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;Text Text="      £2.59"&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;/TextLine&gt;&lt;/TextObject&gt;&lt;TextObject OutputOptions="All"&gt;&lt;TextLine&gt;&lt;LinePrintAttributes Align="Left" CPL="42" /&gt;&lt;Text Text="    "&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;Text Text="*JS CLAS DT COLA2LX4 "&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;Text Text="      £1.99"&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;/TextLine&gt;&lt;/TextObject&gt;&lt;TextObject OutputOptions="All"&gt;&lt;TextLine&gt;&lt;LinePrintAttributes Align="Left" CPL="42" /&gt;&lt;Text Text="    "&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;Text Text="CND S/SKM MLK X6     "&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;Text Text="      £1.99"&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;/TextLine&gt;&lt;/TextObject&gt;&lt;TextObject OutputOptions="All"&gt;&lt;TextLine&gt;&lt;LinePrintAttributes Align="Left" CPL="42" /&gt;&lt;Text Text=""&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;/TextLine&gt;&lt;/TextObject&gt;&lt;TextObject OutputOptions="All"&gt;&lt;TextLine&gt;&lt;LinePrintAttributes Align="Left" CPL="42" /&gt;&lt;Text Text=" "&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;Text Text="   3"&gt;&lt;PrintAttributes Bold="true" /&gt;&lt;/Text&gt;&lt;Text Text=" "&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;Text Text="BALANCE DUE         "&gt;&lt;PrintAttributes Bold="true" /&gt;&lt;/Text&gt;&lt;Text Text="     £6.57"&gt;&lt;PrintAttributes Bold="true" /&gt;&lt;/Text&gt;&lt;/TextLine&gt;&lt;/TextObject&gt;&lt;TextObject OutputOptions="All"&gt;&lt;TextLine&gt;&lt;LinePrintAttributes Align="Left" CPL="42" /&gt;&lt;Text Text="      "&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;Text Text="CASH                "&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;Text Text="     £6.57"&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;/TextLine&gt;&lt;/TextObject&gt;&lt;TextObject OutputOptions="All"&gt;&lt;TextLine&gt;&lt;LinePrintAttributes Align="Left" CPL="42" /&gt;&lt;Text Text=""&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;/TextLine&gt;&lt;/TextObject&gt;&lt;TextObject OutputOptions="All"&gt;&lt;TextLine&gt;&lt;LinePrintAttributes Align="Left" CPL="42" /&gt;&lt;Text Text=""&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;/TextLine&gt;&lt;/TextObject&gt;&lt;TextObject OutputOptions="All"&gt;&lt;TextLine&gt;&lt;LinePrintAttributes Align="Left" CPL="42" /&gt;&lt;Text Text="      "&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;Text Text="CHANGE              "&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;Text Text="     £0.00"&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;/TextLine&gt;&lt;/TextObject&gt;&lt;TextObject OutputOptions="All"&gt;&lt;TextLine&gt;&lt;LinePrintAttributes Align="Center" CPL="42" /&gt;&lt;Text Text="****************************************"&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;/TextLine&gt;&lt;/TextObject&gt;&lt;TextObject OutputOptions="All"&gt;&lt;TextLine&gt;&lt;LinePrintAttributes Align="Center" CPL="42" /&gt;&lt;Text Text="WITH NECTAR YOU WOULD HAVE EARNED:      "&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;/TextLine&gt;&lt;/TextObject&gt;&lt;TextObject OutputOptions="All"&gt;&lt;TextLine&gt;&lt;LinePrintAttributes Align="Center" CPL="42" /&gt;&lt;Text Text="POINTS"&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;Text Text="                           6"&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;Text Text="      "&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;/TextLine&gt;&lt;/TextObject&gt;&lt;TextObject OutputOptions="All"&gt;&lt;TextLine&gt;&lt;LinePrintAttributes Align="Center" CPL="42" /&gt;&lt;Text Text="REGISTER AT www.nectar.com              "&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;/TextLine&gt;&lt;/TextObject&gt;&lt;TextObject OutputOptions="All"&gt;&lt;TextLine&gt;&lt;LinePrintAttributes Align="Center" CPL="42" /&gt;&lt;Text Text="****************************************"&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;/TextLine&gt;&lt;/TextObject&gt;&lt;TextObject OutputOptions="All"&gt;&lt;TextLine&gt;&lt;LinePrintAttributes Align="Left" CPL="42" /&gt;&lt;Text Text=""&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;/TextLine&gt;&lt;/TextObject&gt;&lt;BarcodeObject ID="6290610140045100220816" BarcodeType="Code128" Height="100" Width="1" Align="Center" OutputOptions="All" /&gt;&lt;TextObject OutputOptions="All"&gt;&lt;TextLine&gt;&lt;LinePrintAttributes Align="Center" CPL="42" /&gt;&lt;Text Text="6290610140045100220816"&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;/TextLine&gt;&lt;/TextObject&gt;&lt;TextObject OutputOptions="All"&gt;&lt;TextLine&gt;&lt;LinePrintAttributes Align="Left" CPL="42" /&gt;&lt;Text Text=""&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;/TextLine&gt;&lt;/TextObject&gt;&lt;TextObject OutputOptions="All"&gt;&lt;TextLine&gt;&lt;LinePrintAttributes Align="Left" CPL="42" /&gt;&lt;Text Text="   "&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;Text Text="C"&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;Text Text="9009  "&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;Text Text="  "&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;Text Text="#"&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;Text Text="451 "&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;Text Text="   "&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;Text Text=" 15:21:55 "&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;Text Text=" 22AUG2016"&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;/TextLine&gt;&lt;/TextObject&gt;&lt;TextObject OutputOptions="All"&gt;&lt;TextLine&gt;&lt;LinePrintAttributes Align="Left" CPL="42" /&gt;&lt;Text Text="            "&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;Text Text="S"&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;Text Text="0610 "&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;Text Text="    "&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;Text Text="R"&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;Text Text="140"&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;/TextLine&gt;&lt;/TextObject&gt;&lt;TextObject OutputOptions="All"&gt;&lt;TextLine&gt;&lt;LinePrintAttributes Align="Left" CPL="42" /&gt;&lt;Text Text=""&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;/TextLine&gt;&lt;/TextObject&gt;&lt;TextObject OutputOptions="All"&gt;&lt;TextLine&gt;&lt;LinePrintAttributes Align="Center" CPL="42" /&gt;&lt;Text Text="Thank you for your visit. Please let us"&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;/TextLine&gt;&lt;/TextObject&gt;&lt;TextObject OutputOptions="All"&gt;&lt;TextLine&gt;&lt;LinePrintAttributes Align="Center" CPL="42" /&gt;&lt;Text Text="know how we did at tellsainsburys.co.uk"&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;/TextLine&gt;&lt;/TextObject&gt;&lt;TextObject OutputOptions="All"&gt;&lt;TextLine&gt;&lt;LinePrintAttributes Align="Left" CPL="42" /&gt;&lt;Text Text=""&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;/TextLine&gt;&lt;/TextObject&gt;&lt;TextObject OutputOptions="All"&gt;&lt;TextLine&gt;&lt;LinePrintAttributes Align="Center" CPL="42" /&gt;&lt;Text Text="You can live well for less than you"&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;/TextLine&gt;&lt;/TextObject&gt;&lt;TextObject OutputOptions="All"&gt;&lt;TextLine&gt;&lt;LinePrintAttributes Align="Center" CPL="42" /&gt;&lt;Text Text="thought at Sainsbury's, based on price"&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;/TextLine&gt;&lt;/TextObject&gt;&lt;TextObject OutputOptions="All"&gt;&lt;TextLine&gt;&lt;LinePrintAttributes Align="Center" CPL="42" /&gt;&lt;Text Text="perception data. For more information"&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;/TextLine&gt;&lt;/TextObject&gt;&lt;TextObject OutputOptions="All"&gt;&lt;TextLine&gt;&lt;LinePrintAttributes Align="Center" CPL="42" /&gt;&lt;Text Text="go to sainsburys.co.uk/livewellforless"&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;/TextLine&gt;&lt;/TextObject&gt;&lt;TextObject OutputOptions="All"&gt;&lt;TextLine&gt;&lt;LinePrintAttributes Align="Left" CPL="42" /&gt;&lt;Text Text=""&gt;&lt;PrintAttributes /&gt;&lt;/Text&gt;&lt;/TextLine&gt;&lt;/TextObject&gt;&lt;/PrintSection&gt;&lt;PrintSection Layout="Portrait" SectionID="FeedAndCut"&gt;&lt;PrintCommand Command="FeedPaper" CmdData="6" /&gt;&lt;PrintCommand Command="CutPaper" CmdData="50" /&gt;&lt;PrintCommand Command="FeedPaper" CmdData="1" /&gt;&lt;/PrintSection&gt;&lt;/Document&gt;&lt;/Documents&gt;&lt;/OPOSPrint&gt;</ReceiptLine>
    </ReceiptImage>
    <RetailTransaction>
      <LineItem EntryMethod="Keyed">
        <Sale>
          <POSIdentity>
            <POSItemID>7583599</POSItemID>
          </POSIdentity>
          <MerchandiseHierarchy ID="Merchandise" r10Ex:HierarchyPath="1_09_4111@2_09_1955@3_09_419">3_09_419</MerchandiseHierarchy>
          <MerchandiseHierarchy ID="Tax">0.00</MerchandiseHierarchy>
          <ItemID Type="X:EAN">48</ItemID>
          <Description r10Ex:Culture="en-GB">YRKSH PRK/CHLLI BRGR</Description>
          <Description TypeCode="Long" r10Ex:Culture="en-GB">Real Yorkshire Pork/Chilli Burgers 340g</Description>
          <RegularSalesUnitPrice Currency="GBP">2.59</RegularSalesUnitPrice>
          <ActualSalesUnitPrice Currency="GBP">2.59</ActualSalesUnitPrice>
          <ExtendedAmount Currency="GBP">2.59</ExtendedAmount>
          <Quantity EntryMethod="Automatic" UnitOfMeasureCode="EA" Units="1">1</Quantity>
          <Tax r10Ex:Imposition="A" r10Ex:Sign="A">
            <SequenceNumber>1</SequenceNumber>
            <TaxAuthority>VAT</TaxAuthority>
            <TaxableAmount Currency="GBP" TaxIncludedInTaxableAmountFlag="true">2.59</TaxableAmount>
            <Amount Currency="GBP">0.00</Amount>
            <Percent>0.0000</Percent>
            <TaxRuleID>2</TaxRuleID>
            <TaxGroupID>1</TaxGroupID>
          </Tax>
          <ConsumableGroup xmlns="http://www.Retalix.com/Extensions">
            <Type>Consumable</Type>
            <ID>SalesGroup_SSL</ID>
          </ConsumableGroup>
        </Sale>
        <ScanData>48</ScanData>
        <SequenceNumber>1</SequenceNumber>
        <BeginDateTime>2016-08-22T15:21:49.4509505+01:00</BeginDateTime>
        <EndDateTime>2016-08-22T15:21:49.4509505+01:00</EndDateTime>
      </LineItem>
      <LineItem EntryMethod="Keyed">
        <Sale>
          <POSIdentity>
            <POSItemID>1329421</POSItemID>
          </POSIdentity>
          <MerchandiseHierarchy ID="Merchandise" r10Ex:HierarchyPath="1_09_5418@2_09_156@3_09_291">3_09_291</MerchandiseHierarchy>
          <MerchandiseHierarchy ID="Tax">20.00</MerchandiseHierarchy>
          <ItemID Type="X:EAN">123</ItemID>
          <Description r10Ex:Culture="en-GB">*JS CLAS DT COLA2LX4</Description>
          <Description TypeCode="Long" r10Ex:Culture="en-GB">*JS CLAS DT COLA2LX4</Description>
          <RegularSalesUnitPrice Currency="GBP">1.99</RegularSalesUnitPrice>
          <ActualSalesUnitPrice Currency="GBP">1.99</ActualSalesUnitPrice>
          <ExtendedAmount Currency="GBP">1.99</ExtendedAmount>
          <Quantity EntryMethod="Automatic" UnitOfMeasureCode="EA" Units="1">1</Quantity>
          <Quantity EntryMethod="Automatic" UnitOfMeasureCode="Kg" Units="0.127">0.127</Quantity>
          <Tax r10Ex:Imposition="C" r10Ex:Sign="C">
            <SequenceNumber>1</SequenceNumber>
            <TaxAuthority>VAT</TaxAuthority>
            <TaxableAmount Currency="GBP" TaxIncludedInTaxableAmountFlag="true">1.66</TaxableAmount>
            <Amount Currency="GBP">0.33</Amount>
            <Percent>20.0000</Percent>
            <TaxRuleID>82</TaxRuleID>
            <TaxGroupID>1</TaxGroupID>
          </Tax>
          <ConsumableGroup xmlns="http://www.Retalix.com/Extensions">
            <Type>Consumable</Type>
            <ID>SalesGroup_SSL</ID>
          </ConsumableGroup>
        </Sale>
        <ScanData>123</ScanData>
        <SequenceNumber>2</SequenceNumber>
        <BeginDateTime>2016-08-22T15:21:50.9207335+01:00</BeginDateTime>
        <EndDateTime>2016-08-22T15:21:50.9207335+01:00</EndDateTime>
      </LineItem>
      <LineItem EntryMethod="Keyed">
        <Sale NotNormallyStockedFlag="true">
          <POSIdentity>
            <POSItemID>3545058</POSItemID>
          </POSIdentity>
          <MerchandiseHierarchy ID="Merchandise" r10Ex:HierarchyPath="1_09_5554@2_09_1059@3_09_102">3_09_102</MerchandiseHierarchy>
          <MerchandiseHierarchy ID="Tax">20.00</MerchandiseHierarchy>
          <ItemID Type="X:EAN">321</ItemID>
          <Description r10Ex:Culture="en-GB">CND S/SKM MLK X6    </Description>
          <Description TypeCode="Long" r10Ex:Culture="en-GB">CND S/SKM MLK X6</Description>
          <RegularSalesUnitPrice Currency="GBP">1.99</RegularSalesUnitPrice>
          <ActualSalesUnitPrice Currency="GBP">1.99</ActualSalesUnitPrice>
          <ExtendedAmount Currency="GBP">1.99</ExtendedAmount>
          <Quantity EntryMethod="Automatic" UnitOfMeasureCode="EA" Units="1">1</Quantity>
          <Tax r10Ex:Imposition="C" r10Ex:Sign="C">
            <SequenceNumber>1</SequenceNumber>
            <TaxAuthority>VAT</TaxAuthority>
            <TaxableAmount Currency="GBP" TaxIncludedInTaxableAmountFlag="true">1.66</TaxableAmount>
            <Amount Currency="GBP">0.33</Amount>
            <Percent>20.0000</Percent>
            <TaxRuleID>82</TaxRuleID>
            <TaxGroupID>1</TaxGroupID>
          </Tax>
          <ConsumableGroup xmlns="http://www.Retalix.com/Extensions">
            <Type>Consumable</Type>
            <ID>SalesGroup_SSL</ID>
          </ConsumableGroup>
        </Sale>
        <ScanData>321</ScanData>
        <SequenceNumber>3</SequenceNumber>
        <BeginDateTime>2016-08-22T15:21:52.1825007+01:00</BeginDateTime>
        <EndDateTime>2016-08-22T15:21:52.1825007+01:00</EndDateTime>
      </LineItem>
      <LineItem>
        <SequenceNumber>4</SequenceNumber>
        <BeginDateTime>2016-08-22T15:21:52.6971689+01:00</BeginDateTime>
        <EndDateTime>2016-08-22T15:21:52.6971689+01:00</EndDateTime>
        <TotalKey xmlns="http://www.Retalix.com/Extensions" Type="SubTotal">
          <Total TotalType="TransactionGrossAmount">6.57</Total>
          <Total TotalType="TransactionNetAmount">6.57</Total>
          <Total TotalType="TransactionPurchaseQuantity">3</Total>
        </TotalKey>
      </LineItem>
      <LineItem EntryMethod="Tapped">
        <Tender TenderType="Cash">
          <Amount Currency="GBP">6.57</Amount>
          <Cashback Currency="GBP">0</Cashback>
          <TenderID>1</TenderID>
          <r10Ex:TenderDescription>CASH</r10Ex:TenderDescription>
          <r10Ex:IsAutoReconcile>False</r10Ex:IsAutoReconcile>
        </Tender>
        <SequenceNumber>5</SequenceNumber>
        <BeginDateTime>2016-08-22T15:21:54.4716511+01:00</BeginDateTime>
        <EndDateTime>2016-08-22T15:21:54.4716511+01:00</EndDateTime>
      </LineItem>
      <LineItem>
        <Tax r10Ex:Imposition="A">
          <SequenceNumber>1</SequenceNumber>
          <TaxAuthority>VAT</TaxAuthority>
          <TaxableAmount Currency="GBP" TaxIncludedInTaxableAmountFlag="true">2.59</TaxableAmount>
          <Amount Currency="GBP">0.00</Amount>
          <Percent>0.0000</Percent>
          <TaxRuleID>2</TaxRuleID>
          <TaxGroupID>1</TaxGroupID>
        </Tax>
        <SequenceNumber>6</SequenceNumber>
      </LineItem>
      <LineItem>
        <Tax r10Ex:Imposition="C">
          <SequenceNumber>2</SequenceNumber>
          <TaxAuthority>VAT</TaxAuthority>
          <TaxableAmount Currency="GBP" TaxIncludedInTaxableAmountFlag="true">3.32</TaxableAmount>
          <Amount Currency="GBP">0.66</Amount>
          <Percent>20.0000</Percent>
          <TaxRuleID>82</TaxRuleID>
          <TaxGroupID>1</TaxGroupID>
        </Tax>
        <SequenceNumber>7</SequenceNumber>
      </LineItem>
      <Total CurrencyCode="GBP">6.57</Total>
      <Total CurrencyCode="GBP" TotalType="TransactionNetAmount">6.57</Total>
      <Total TotalType="TransactionPurchaseQuantity">3</Total>
      <Total TotalType="TransactionPurchaseQuantity" TypeCode="Return">0</Total>
      <Total CurrencyCode="GBP" TotalType="X:TransactionTaxIncluded">0.66</Total>
      <Total CurrencyCode="GBP" TotalType="X:TransactionTaxSurcharge">0</Total>
      <Total CurrencyCode="GBP" TotalType="X:TransactionTaxFee">0</Total>
      <Total CurrencyCode="GBP" TotalType="X:ItemTaxFee">0</Total>
      <Total CurrencyCode="GBP" TotalType="TransactionTotalSavings">0</Total>
      <Total CurrencyCode="GBP" TotalType="TransactionGrandAmount">6.57</Total>
      <Total CurrencyCode="GBP" TotalType="X:TransactionTotalVoidAmount">0</Total>
      <Total TotalType="X:TransactionTotalVoidCount">0</Total>
      <Total CurrencyCode="GBP" TotalType="X:TransactionTotalReturnAmount">0</Total>
      <Total CurrencyCode="GBP" TotalType="TransactionTaxExemptAmount">0</Total>
      <Total CurrencyCode="GBP" TotalType="TransactionTenderApplied">6.57</Total>
      <Total CurrencyCode="GBP" TotalType="X:CashbackTotalAmount">0</Total>
      <Total CurrencyCode="GBP" TotalType="X:TransactionMerchandiseAmount">6.57</Total>
      <Total CurrencyCode="GBP" TotalType="X:TransactionNonMerchandiseAmount">0</Total>
      <Total CurrencyCode="GBP" TotalType="X:NonResettableGrandTotal">1409.91</Total>
      <Total CurrencyCode="GBP" TotalType="X:PreviousNonResettableGrandTotal">1398.26</Total>
      <Customer r10Ex:IsAuthenticatedOffline="False">
        <Name/>
      </Customer>
      <TaxDefinitions xmlns="http://www.Retalix.com/Extensions" xmlns:xsd="http://www.w3.org/2001/XMLSchema">
        <TaxAuthority>
          <AuthorityId>1</AuthorityId>
          <Descriptions>
            <Description Culture="en-GB">VAT</Description>
          </Descriptions>
        </TaxAuthority>
        <TaxRate>
          <RateId>2</RateId>
          <Type>Tax</Type>
          <AuthorityId>1</AuthorityId>
          <Descriptions>
            <Description Culture="en-GB">0.00%</Description>
          </Descriptions>
          <IsIncluded>true</IsIncluded>
          <TaxCalculatedMethodPercent>
            <Value>0.0000</Value>
            <ImpositionId>A</ImpositionId>
          </TaxCalculatedMethodPercent>
          <TaxIndicator>A</TaxIndicator>
          <RoundingType>Standard</RoundingType>
          <CouponReducesTaxationAmount>false</CouponReducesTaxationAmount>
        </TaxRate>
        <TaxRate>
          <RateId>82</RateId>
          <Type>Tax</Type>
          <AuthorityId>1</AuthorityId>
          <Descriptions>
            <Description Culture="en-GB">20.00%</Description>
          </Descriptions>
          <IsIncluded>true</IsIncluded>
          <TaxCalculatedMethodPercent>
            <Value>20.0000</Value>
            <ImpositionId>C</ImpositionId>
          </TaxCalculatedMethodPercent>
          <TaxIndicator>C</TaxIndicator>
          <RoundingType>Standard</RoundingType>
          <CouponReducesTaxationAmount>false</CouponReducesTaxationAmount>
        </TaxRate>
      </TaxDefinitions>
      <TransactionMeasures xmlns="http://www.Retalix.com/Extensions" xmlns:xsd="http://www.w3.org/2001/XMLSchema">
        <ItemRingTime>
          <Current>9</Current>
          <BusinessDayDateTotal>13</BusinessDayDateTotal>
        </ItemRingTime>
        <SecureTime>
          <Current>0</Current>
          <BusinessDayDateTotal>0</BusinessDayDateTotal>
        </SecureTime>
        <IdleTime>
          <Current>1</Current>
          <BusinessDayDateTotal>2</BusinessDayDateTotal>
        </IdleTime>
        <TenderTime>
          <Current>0</Current>
          <BusinessDayDateTotal>0</BusinessDayDateTotal>
        </TenderTime>
        <OtherTime>
          <Current>0</Current>
          <BusinessDayDateTotal>4</BusinessDayDateTotal>
        </OtherTime>
        <SignOnTime>
          <Current>10</Current>
          <BusinessDayDateTotal>19</BusinessDayDateTotal>
        </SignOnTime>
      </TransactionMeasures>
    </RetailTransaction>
    <BusinessDayDate>2016-08-22</BusinessDayDate>
    <BeginDateTime>2016-08-22T15:21:49.3991907+01:00</BeginDateTime>
    <EndDateTime>2016-08-22T15:21:54.4833703+01:00</EndDateTime>
  </Transaction>
</POSLog>`,
	}
)

func main() {

	flag.Parse()

	amqp09.DeclareExchange(*uri, *exchange, *exchangeType, *queue)

	pipeline := goc.NewPipeline().WithCoders(coder.Registry())

	pipeline.
		Root(io.RoundRobin(100000, data)).
		Apply(&amqp09.Sink{Uri: *uri, Exchange: *exchange, BindingKey: *bindingKey,})

	pipeline.Run()

}
