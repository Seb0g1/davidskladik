import type { ReactNode } from "react";
import { PageTabsStrip, usePageTabs } from "./PageTabs";

// Inside a tabbed section (see TabbedPage) the header shows the section title and its tabs;
// the page's own title then becomes the line under it.
export function PageHeader({ title, subtitle, action }: { title: string; subtitle: string; action?: ReactNode }) {
  const tabs = usePageTabs();
  return (
    <section className={`page-heading${tabs ? " has-tabs" : ""}`}>
      <div>
        <span className="eyebrow">ДавидСклад 2.0</span>
        <h1>{tabs ? tabs.title : title}</h1>
        <p>{subtitle}</p>
      </div>
      {action}
      <PageTabsStrip />
    </section>
  );
}
