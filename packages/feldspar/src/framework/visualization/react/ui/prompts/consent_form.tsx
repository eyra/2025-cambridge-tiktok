import React, { JSX, useState } from "react";
import {
  PropsUIPromptConsentForm,
  PropsUIPromptConsentFormTable,
} from "../../../../types/prompts";
import { Translator } from "../../../../translator";
import { PromptContext } from "./factory";
import { ConsentTable } from "./consent_table";
import { PropsUITable } from "../../../../types/elements";

interface ConsentFormProps extends PropsUIPromptConsentForm, PromptContext {}

export const ConsentForm = (props: ConsentFormProps): JSX.Element => {
  const { tables, donateQuestion, donateButton, locale, onDonate } = props;
  const [consented, setConsented] = useState(false);

  const handleConsent = () => {
    setConsented(true);
    if (onDonate) {
      onDonate();
    }
  };

  return (
    <div style={{ marginTop: "16px" }}>
      {tables.map(
        (tableProps: PropsUIPromptConsentFormTable, index: number) => {
          const { id, title, description, data_frame } = tableProps;
          const dataFrame = JSON.parse(data_frame);

          const headCells = Object.keys(dataFrame).map((column: string) => ({
            __type__: "PropsUITableCell" as const,
            text: column,
          }));
          const head = {
            __type__: "PropsUITableHead" as const,
            cells: headCells,
          };

          const rows = Object.keys(
            dataFrame[Object.keys(dataFrame)[0]] || {}
          ).map((rowIndex) => ({
            __type__: "PropsUITableRow" as const,
            id: rowIndex,
            cells: Object.keys(dataFrame).map((column) => ({
              __type__: "PropsUITableCell" as const,
              text: String(dataFrame[column][rowIndex]),
            })),
          }));

          const tableBody = { __type__: "PropsUITableBody" as const, rows };

          const parsedTable: PropsUITable = {
            __type__: "PropsUITable" as const,
            id,
            head,
            body: tableBody,
          };

          return (
            <div key={id} style={{ marginBottom: "32px" }}>
              <h2 style={{ marginBottom: "8px" }}>
                {Translator.translate(title, locale)}
              </h2>
              <p style={{ marginBottom: "16px" }}>
                {Translator.translate(description, locale)}
              </p>
              <ConsentTable
                table={{
                  ...parsedTable,
                  title: Translator.translate(title, locale),
                  deletedRowCount: 0,
                }}
                context={props}
                onChange={() => {}} // Tables in consent form are read-only
              />
            </div>
          );
        }
      )}

      <div
        style={{ marginTop: "32px", display: "flex", justifyContent: "center" }}
      >
        <button
          style={{
            padding: "8px 16px",
            backgroundColor: "#1976d2",
            color: "white",
            border: "none",
            borderRadius: "4px",
            cursor: consented ? "default" : "pointer",
            opacity: consented ? 0.7 : 1,
          }}
          onClick={handleConsent}
          disabled={consented}
        >
          {donateButton ? Translator.translate(donateButton, locale) : "Donate"}
        </button>
      </div>
    </div>
  );
};
